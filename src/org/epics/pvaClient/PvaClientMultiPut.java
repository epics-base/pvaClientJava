/**
 * 
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.ChannelPut;
import org.epics.pvaccess.client.ChannelPutRequester;
import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.PVUnionArray;
import org.epics.pvdata.pv.Scalar;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.Type;
import org.epics.pvdata.pv.UnionArrayData;

/**
 * An easy way to put data to multiple channels.
 * @author mrk
 *
 */
public class PvaClientMultiPut implements ChannelPutRequester {
    /**
     * Create a new PvaClientMultiPut.
     * @return The interface.
     */
    static PvaClientMultiPut create(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            boolean doubleOnly)
    {
        PvaClientMultiPut multiPut =   new PvaClientMultiPut(
                easyMultiChannel,channel,doubleOnly);
        if(multiPut.init()) return multiPut;
        return null;
    }
    PvaClientMultiPut(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            Boolean doubleOnly)
            {
        this.easyMultiChannel = easyMultiChannel;
        this.channel = channel;
        this.doubleOnly = doubleOnly;
        nchannel = channel.length;
        isConnected = new boolean[nchannel];
        for(int i=0; i<nchannel; ++i) isConnected[i] = false;
            }
    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();
    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    private final PvaClientMultiChannel easyMultiChannel;
    private final Channel[] channel;
    private boolean doubleOnly;
    private final int nchannel;

    // following initialized by init and 

    private volatile PVStructure[] topPVStructure = null;
    private volatile BitSet[] putBitSet = null;
    private volatile ChannelPut[] channelPut = null;
    private PvaClientMultiGet easyMultiGet = null;


    // following used by connect
    private volatile int numConnectCallback = 0;
    private volatile int numConnected = 0;
    private volatile boolean[] isConnected = null;
    private enum ConnectState {connectIdle,connectActive,connectDone};
    private volatile ConnectState connectState = ConnectState.connectIdle;

    // following used by put
    private volatile int numPut = 0;
    private volatile boolean badPut = false;
    private volatile boolean illegalPut = false;
    private enum PutState {putIdle,putActive,putFailed,putDone};
    private volatile PutState putState = PutState.putIdle;

    private volatile boolean isDestroyed = false;
    private final PVStructure pvRequest = CreateRequest.create().createRequest("field(value)");
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForPut = lock.newCondition();
    private volatile Status status = statusCreate.getStatusOK();
    private volatile UnionArrayData unionArrayData = new UnionArrayData();

    private boolean init()
    {
        channelPut = new ChannelPut[nchannel];
        topPVStructure = new PVStructure[nchannel];
        easyMultiGet = easyMultiChannel.createGet(doubleOnly, "field(value)");
        putBitSet = new BitSet[nchannel];
        return true;
    }

    private void checkConnected() {
        if(connectState==ConnectState.connectIdle) connect();
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#getRequesterName()
     */
    @Override
    public String getRequesterName() {
        return easyMultiChannel.getRequesterName();
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#message(java.lang.String, org.epics.pvdata.pv.MessageType)
     */
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) return;
        easyMultiChannel.message(message, messageType);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#channelPutConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void channelPutConnect(Status status, ChannelPut channelPut,Structure structure) {
        if(isDestroyed) return;
        int index = -1;
        for(int i=0; i<channel.length; ++i) {
            if(easyMultiChannel.getChannelNames()[i].equals(channelPut.getChannel().getChannelName())) {
                index = i;
                break;
            }
        }
        if(index<0) {
            throw new IllegalStateException("should not happen");
        }
        this.channelPut[index] = channelPut;
        if(status.isOK()) {
            if(!isConnected[index]) {
                ++numConnected;
                isConnected[index] = true;
                topPVStructure[index] = pvDataCreate.createPVStructure(structure);
                putBitSet[index] = new BitSet(topPVStructure[index].getNumberFields());
                Field field = structure.getField("value");
                if(field==null) {
                    setStatus(statusCreate.createStatus(
                            StatusType.ERROR,"channel " + channel[index].getChannelName()
                            +" does not have top level value field",null));
                } else {
                    boolean success= true;
                    if(doubleOnly) {
                        if(field.getType()!=Type.scalar) {
                            success = false;
                        } else {
                            Scalar scalar = (Scalar)field;
                            if(!scalar.getScalarType().isNumeric()) success = false;
                        }
                        if(!success) {
                            setStatus(statusCreate.createStatus(
                                    StatusType.ERROR,"channel value is not a numeric scalar",null));
                        }
                    }
                }
            }
        } else {
            if(isConnected[index]) {
                --numConnected;
                isConnected[index] = false;
                setStatus(status);
            }
        }
        if(connectState!=ConnectState.connectActive) return;
        lock.lock();
        try {
            numConnectCallback++;
            if(numConnectCallback==nchannel) {
                connectState = ConnectState.connectDone;
                waitForConnect.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#putDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut)
     */
    @Override
    public void putDone(Status status, ChannelPut channelPut) {
        int index = -1;
        for(int i=0; i<channel.length; ++i) {
            if(easyMultiChannel.getChannelNames()[i].equals(channelPut.getChannel().getChannelName())) {
                index = i;
                break;
            }
        }
        if(index<0) {
            throw new IllegalStateException("should not happen");
        }
        if(!status.isOK()) {
            badPut = true;
        }
        lock.lock();
        try {
            ++numPut;
            if(numPut==nchannel) {
                if(badPut) {
                    putState = PutState.putFailed;
                } else {
                    putState = PutState.putDone;
                }
                waitForPut.signal();
            }
        } finally {
            lock.unlock();
        }
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#getDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void getDone(Status status, ChannelPut channelPut,PVStructure pvStructure, BitSet bitSet) {
        // using EasyMultiGet so this not used.

    }



    /**
     * Clean up
     */
    void destroy()
    {
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        easyMultiGet.destroy();
        for(int i=0; i<nchannel; ++i) {
            if(channelPut[i]!=null) channelPut[i].destroy();
        }
    }

    /**
     * Calls issueConnect and then waitConnect.
     * @return (false,true) if (failure, success)
     */
    boolean connect()
    {
        issueConnect();
        return waitConnect();
    }

    /**
     * create the channelPut for all channels.
     */
    void issueConnect()
    {
        if(isDestroyed) return;
        if(connectState!=ConnectState.connectIdle) {
            Status status = statusCreate.createStatus(
                    StatusType.ERROR,"connect already issued",null);
            setStatus(status);
            return;
        }
        numConnectCallback = 0;
        connectState = ConnectState.connectActive;
        for(int i=0; i<channel.length; ++i) channelPut[i] = channel[i].createChannelPut(this, pvRequest);
    }
    /**
     * Wait until all channelPuts are created.
     * @return (false,true) if (failure, success)
     */
    boolean waitConnect()
    {
        if(isDestroyed) return false;
        try {
            lock.lock();
            try {
                if(numConnectCallback<nchannel) waitForConnect.await();
            } catch(InterruptedException e) {
                Status status = statusCreate.createStatus(
                        StatusType.ERROR,
                        e.getMessage(),
                        e.fillInStackTrace());
                setStatus(status);
                return false;
            }
        } finally {
            lock.unlock();
        }
        if(numConnected!=nchannel) {
            Status status = statusCreate.createStatus(StatusType.ERROR," did not connect",null);
            setStatus(status);
            return false;
        }
        return true;
    }
    /**
     * call issueGet and the waitGet.
     * @return (false,true) if (failure, success)
     */
    boolean get()
    {
        return easyMultiGet.get();
    }
    /**
     * Issue a get for each channel.
     */
    void issueGet()
    {
        easyMultiGet.issueGet();
    }

    /**
     * wait until all gets are complete.
     * @return (true,false) if (no errors, errors) resulted from gets.
     * If an error occurred then getStatus returns a reason.
     * @return (false,true) if (failure, success)
     */
    boolean waitGet()
    {
        return easyMultiGet.waitGet();
    }
    /**
     * Get the number of channels.
     * @return The number of channels.
     */
    int getLength()
    {
        return nchannel;
    }
    /**
     * Is value a double[] ?
     * @return The answer.
     */
    boolean doubleOnly()
    {
        return doubleOnly;
    }
    /**
     * Get the value field as a MTMultiChannel structure.
     * @return The value.
     * This is null if doubleOnly is true.
     */
    PVStructure getNTMultiChannel()
    {
        if(doubleOnly) return null;
        return easyMultiGet.getNTMultiChannel();
    }
    /**
     * Get the top level structure of the value field as a double[[]
     * @return The top level structure.
     * This is null if doubleOnly is false.
     */
    PVStructure getPVTop()
    {
        return easyMultiGet.getPVTop();
    }
    /**
     * Return the value field.
     * @return The double[]
     * This is null if doubleOnly is false.
     */
    double[] getDoubleArray()
    {
        return easyMultiGet.getDoubleArray();
    }
    /**
     * Get the data from the value field.
     * @param offset The offset into the data of the value field.
     * @param data The place to copy the data.
     * @param length The number of elements to copy.
     * @return The number of elements copied.
     * This is 0 if doubleOnly is false.
     */
    int getDoubleArray(int offset, double[]data,int length)
    {
        return easyMultiGet.getDoubleArray(index,data,length);
    }

    /**
     * Call issuePut and then waitPut.
     * @param pvNTMultiChannel The pvStructure for an NTMultiChannel.
     * @return (false,true) means (failure,success)
     */
    boolean put(PVStructure pvNTMultiChannel)
    {
        issuePut(pvNTMultiChannel);
        return waitPut();
    }
    /**
     * Put the value field as a NTMultiChannel.
     * @param pvNTMultiChannel The pvStructure for an NTMultiChannel.
     */
    void issuePut(PVStructure pvNTMultiChannel)
    {
        if(doubleOnly) {
            illegalPut = true;
            return;
        }
        checkConnected();
        if(putState!=PutState.putIdle) {
            Status status = statusCreate.createStatus(
                    StatusType.ERROR,"put already issued",null);
            setStatus(status);
            return;
        }
        boolean allConnected = true;
        for(int i=0; i<nchannel; ++i) if(!channelPut[i].getChannel().isConnected()) allConnected = false;
        if(!allConnected) {
            badPut = true;
            return;
        }
        illegalPut = false;
        numPut = 0;
        badPut = false;
        putState = PutState.putActive;
        PVUnionArray pvArray = pvNTMultiChannel.getUnionArrayField("value");
        pvArray.get(0, nchannel, unionArrayData);
        for(int i=0; i<nchannel; ++i) {
            PVStructure top = topPVStructure[i];
            PVField pvTo = top.getSubField("value");
            PVUnion pvUnion = unionArrayData.data[i];
            PVField pvFrom = pvUnion.get();
            if(convert.isCopyCompatible(pvFrom.getField(),pvTo.getField())) {
                convert.copy(pvFrom, pvTo);
                putBitSet[i].clear();
                putBitSet[i].set(0);
                channelPut[i].put(top, putBitSet[i]);
            } else {
                String message = "channel " + channel[i].getChannelName();
                message += " can not copy value";
                setStatus(statusCreate.createStatus(StatusType.ERROR,message,null));
            }
        }
    }
    /**
     * Call issuePut and then waitPut.
     * @param value The value for each channel.
     * @return (false,true) means (failure,success)
     */
    boolean put(double[] value)
    {
        issuePut(value);
        return waitPut();
    }
    /**
     * Put the value field as a double array.
     * @param value The value for each channel.
     */
    void issuePut(double[] value)
    {
        if(!doubleOnly) {
            illegalPut = true;
            return;
        }
        checkConnected();
        if(putState!=PutState.putIdle) {
            Status status = statusCreate.createStatus(
                    StatusType.ERROR,"put already issued",null);
            setStatus(status);
            return;
        }
        boolean allConnected = true;
        for(int i=0; i<nchannel; ++i) if(!channelPut[i].getChannel().isConnected()) allConnected = false;
        if(!allConnected) {
            badPut = true;
            return;
        }
        illegalPut = false;
        numPut = 0;
        badPut = false;
        putState = PutState.putActive;
        for(int i=0; i<nchannel; ++i) {
            PVStructure top = topPVStructure[i];
            PVScalar pvScalar = top.getSubField(PVScalar.class,"value");
            convert.fromDouble(pvScalar,value[i]);
            putBitSet[i].clear();
            putBitSet[i].set(0);
            channelPut[i].put(top, putBitSet[i]);
        }
    }
    /**
     * Wait for the put to complete.
     * 
     * @return (true,false) if (no errors, errors) resulted from puts.
     * If an error occurred then getStatus returns a reason.
     */
    boolean waitPut()
    {
        if(isDestroyed) return false;
        checkConnected();
        if(illegalPut) {
            Status status = statusCreate.createStatus(StatusType.ERROR,"illegal put request", null);
            setStatus(status);
            return false;
        }
        try {
            lock.lock();
            try {
                if(putState==PutState.putActive) waitForPut.await();
            } catch(InterruptedException e) {
                Status status = statusCreate.createStatus(StatusType.ERROR, e.getMessage(), e.fillInStackTrace());
                setStatus(status);
                return false;
            }
        } finally {
            lock.unlock();
        }
        putState = PutState.putIdle;
        if(badPut) {
            Status status = statusCreate.createStatus(StatusType.ERROR," put failed",null);
            setStatus(status);
            return false;
        }
        return true;
    }
}
