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
public class PvaClientMultiPut {
    /**
     * Create a new PvaClientMultiPut.
     * @return The interface.
     */
    static PvaClientMultiPut create(
            PvaClientMultiChannel pvaClientMultiChannel,
            Channel[] channel,
            boolean doubleOnly)
    {
        return new PvaClientMultiPut(pvaClientMultiChannel,channel,doubleOnly);
    }

    private PvaClientMultiPut(
            PvaClientMultiChannel pvaClientMultiChannel,
            Channel[] channel,
            Boolean doubleOnly)
   {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.channel = channel;
        this.doubleOnly = doubleOnly;
        nchannel = channel.length;
        channelPutRequester = new ChannelPutRequesterPvt[nchannel];
        isConnected = new boolean[nchannel];
        channelPut = new ChannelPut[nchannel];
        topPVStructure = new PVStructure[nchannel];
        putBitSet = new BitSet[nchannel];
        for(int i=0; i<nchannel; ++i) {
            channelPutRequester[i] = new ChannelPutRequesterPvt(this,i);
            isConnected[i] = false;
            channelPut[i] = null;
            if(channel[i].isConnected()) ++numPutToConnect;
        }
        pvaClientMultiGet = pvaClientMultiChannel.createGet(doubleOnly, "field(value)");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    
    private static class ChannelPutRequesterPvt implements ChannelPutRequester
    {
        private final PvaClientMultiPut multiPut;
        private final int indChannel;

        ChannelPutRequesterPvt(PvaClientMultiPut multiPut,int indChannel)
        {
            this.multiPut = multiPut;
            this.indChannel = indChannel;
        }
        public String getRequesterName() {
            return multiPut.pvaClientMultiChannel.getPvaClient().getRequesterName();
        }
        public void message(String message, MessageType messageType) {
            multiPut.pvaClientMultiChannel.getPvaClient().message(message, messageType);
        }
        
        @Override
        public void channelPutConnect(Status status, ChannelPut channelPut,
                Structure structure)
        {
            multiPut.channelPutConnect(status,channelPut,structure,indChannel);
            
        }
        @Override
        public void putDone(Status status, ChannelPut channelPut) {
            multiPut.putDone(status,channelPut,indChannel);
            
        }
        @Override
        public void getDone(Status status, ChannelPut channelPut,
                PVStructure pvStructure, BitSet bitSet) {
            multiPut.getDone(status,channelPut,pvStructure,bitSet,indChannel);
            
        }
    }
    
    private static final Convert convert = ConvertFactory.getConvert();
    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final Channel[] channel;
    private boolean doubleOnly;
    private final int nchannel;

    // following initialized by init and 
    private ChannelPutRequesterPvt[] channelPutRequester = null;
    private volatile PVStructure[] topPVStructure = null;
    private volatile BitSet[] putBitSet = null;
    private volatile ChannelPut[] channelPut = null;
    private PvaClientMultiGet pvaClientMultiGet = null;


    // following used by connect
    private volatile int numPutToConnect = 0;
    private volatile int numConnectCallback = 0;
    private volatile int numConnected = 0;
    private volatile boolean[] isConnected = null;
    private enum ConnectState {connectIdle,connectActive,connectDone};
    private volatile ConnectState connectState = ConnectState.connectIdle;

    // following used by put
    private volatile int numPut = 0;
    private volatile int numPutCallback = 0;
    private volatile boolean badPut = false;
    private volatile boolean illegalPut = false;
    private enum PutState {putIdle,putActive,putFailed,putDone};
    private volatile PutState putState = PutState.putIdle;

    private volatile boolean isDestroyed = false;
    private final PVStructure pvRequest = CreateRequest.create().createRequest("field(value)");
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForPut = lock.newCondition();
    private volatile UnionArrayData unionArrayData = new UnionArrayData();

    private void checkConnected() {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiPut was destroyed");
        if(connectState==ConnectState.connectIdle) connect();
    }

   
    private void channelPutConnect(
            Status status,
            ChannelPut channelPut,
            Structure structure,
            int index)
    {
        if(isDestroyed) return;
        
        this.channelPut[index] = channelPut;
        if(status.isOK()) {
            if(!isConnected[index]) {
                ++numConnected;
                isConnected[index] = true;
                topPVStructure[index] = pvDataCreate.createPVStructure(structure);
                putBitSet[index] = new BitSet(topPVStructure[index].getNumberFields());
                Field field = structure.getField("value");
                if(field==null) {
                    String message = "channel " + channel[index].getChannelName()
                            +" does not have top level value field";
                    throw new RuntimeException(message);
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
                            String message = "channel " + channel[index].getChannelName()
                                    +"  value  is not a numeric scalar";
                            throw new RuntimeException(message);
                        }
                    }
                }
            }
        } else {
            if(isConnected[index]) {
                --numConnected;
                isConnected[index] = false;
                String message = "channel " + channel[index].getChannelName()
                        +" is not connected";
                throw new RuntimeException(message);
            }
        }
        if(connectState!=ConnectState.connectActive) return;
        lock.lock();
        try {
            numConnectCallback++;
            if(numConnectCallback==numPutToConnect) {
                connectState = ConnectState.connectDone;
                waitForConnect.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private void putDone(Status status, ChannelPut channelPut,int index)
    {
       
        if(!status.isOK()) {
            badPut = true;
        }
        lock.lock();
        try {
            ++numPutCallback;
            if(numPutCallback==numPut) {
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
   
   
    private void getDone(
        Status status,
        ChannelPut channelPut,
        PVStructure pvStructure,
        BitSet bitSet,
        int index)
    {
        //  not used.

    }



    /**
     * Clean up
     */
    public void destroy()
    {
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        pvaClientMultiGet.destroy();
        for(int i=0; i<nchannel; ++i) {
            if(channelPut[i]!=null) channelPut[i].destroy();
        }
    }

    /**
     * Calls issueConnect and then waitConnect.
     * If connection is not made an exception is thrown.
     */
    public void connect()
    {
        issueConnect();
        Status status = waitConnect();
        if(!status.isOK()) throw new RuntimeException(status.getMessage());
    }

    /**
     * create the channelPut for all channels.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiPut was destroyed");
        if(connectState!=ConnectState.connectIdle) {
            throw new RuntimeException("pvaClientMultiPut connect already issued");
        }
        numConnectCallback = 0;
        if(pvaClientMultiChannel.allConnected()) {
            numPutToConnect = channel.length;
        } else {
            for(int i=0; i<channel.length; ++i) {
                if(channel[i].isConnected()) numPutToConnect++;
            }
        }
        connectState = ConnectState.connectActive;
        for(int i=0; i<channel.length; ++i) channelPut[i] = channel[i].createChannelPut(
                channelPutRequester[i], pvRequest);
    }
    /**
     * Wait until all channelPuts are created.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiPut was destroyed");
        try {
            lock.lock();
            try {
                if(numConnectCallback<nchannel) waitForConnect.await();
            } catch(InterruptedException e) {
                return statusCreate.createStatus(
                        StatusType.ERROR,"pvaClientMultiPut waitConnect exception " + e.getMessage(),
                        e.fillInStackTrace());
            }
        } finally {
            lock.unlock();
        }
        if(numConnected!=numPutToConnect) {
            return statusCreate.createStatus(StatusType.ERROR," did not connect",null);
        }
        return statusCreate.getStatusOK();
    }
    /**
     * call issueGet and the waitGet.
     * An exception is thrown if get fails
     */
    public void get()
    {
        pvaClientMultiGet.get();
    }
    /**
     * Issue a get for each channel.
     */
    public void issueGet()
    {
        pvaClientMultiGet.issueGet();
    }

    /**
     * wait until all gets are complete.
     * @return status of get request.
     */
    public Status waitGet()
    {
        return pvaClientMultiGet.waitGet();
    }
    /**
     * Get the number of channels.
     * @return The number of channels.
     */
    public int getLength()
    {
        return nchannel;
    }
    /**
     * Is value a double[] ?
     * @return The answer.
     */
    public boolean doubleOnly()
    {
        return doubleOnly;
    }
    /**
     * Get the value field as a MTMultiChannel structure.
     * @return The value.
     */
    public PVStructure getNTMultiChannel()
    {
        if(doubleOnly) return null;
        return pvaClientMultiGet.getNTMultiChannel();
    }
    /**
     * Get the top level structure of the value field as a double[[]
     * @return The top level structure.
     */
    public PVStructure getPVTop()
    {
        return pvaClientMultiGet.getPVTop();
    }
    /**
     * Return the value field.
     * @return The double[]
     */
    public double[] getDoubleArray()
    {
        return pvaClientMultiGet.getDoubleArray();
    }
    /**
     * Get the data from the value field.
     * @param offset The offset into the data of the value field.
     * @param data The place to copy the data.
     * @param length The number of elements to copy.
     * @return The number of elements copied.
     * This is 0 if doubleOnly is false.
     */
    public int getDoubleArray(int offset, double[]data,int length)
    {
        return pvaClientMultiGet.getDoubleArray(offset,data,length);
    }

    /**
     * Call issuePut and then waitPut.
     * @param pvNTMultiChannel The pvStructure for an NTMultiChannel.
     * An exception is thrown if put fails.
     */
    public void put(PVStructure pvNTMultiChannel)
    {
        issuePut(pvNTMultiChannel);
        Status status = waitPut();
        if(!status.isOK()) throw new RuntimeException("pvaClientMultiPut::put " +status.getMessage());
    }
    /**
     * Put the value field as a NTMultiChannel.
     * @param pvNTMultiChannel The pvStructure for an NTMultiChannel.
     */
    public void issuePut(PVStructure pvNTMultiChannel)
    {
        if(doubleOnly) {
            illegalPut = true;
            return;
        }
        checkConnected();
        if(putState!=PutState.putIdle) {
            throw new RuntimeException("pvaClientMultiPut put already issued");
        }
        boolean allConnected = true;
        for(int i=0; i<nchannel; ++i) if(!channelPut[i].getChannel().isConnected()) allConnected = false;
        if(!allConnected) {
            badPut = true;
            return;
        }
        illegalPut = false;
        numPut = 0;
        numPutCallback = 0;
        for(int i=0; i<nchannel; ++i) {
            if(channelPut[i].getChannel().isConnected()) ++numPut;
        }
        badPut = false;
        putState = PutState.putActive;
        PVUnionArray pvArray = pvNTMultiChannel.getUnionArrayField("value");
        pvArray.get(0, nchannel, unionArrayData);
        for(int i=0; i<nchannel; ++i) {
            if(!channelPut[i].getChannel().isConnected()) continue;
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
                throw new RuntimeException("pvaClientMultiPut can not copy value");
            }
        }
    }
    /**
     * Call issuePut and then waitPut.
     * @param value The value for each channel.
     * An exception is thrown if put fails.
     */
    public void put(double[] value)
    {
        issuePut(value);
        Status status = waitPut();
        if(!status.isOK()) throw new RuntimeException("pvaClientMultiPut::put " +status.getMessage());
    }
    /**
     * Put the value field as a double array.
     * @param value The value for each channel.
     */
    public void issuePut(double[] value)
    {
        if(!doubleOnly) {
            illegalPut = true;
            return;
        }
        checkConnected();
        if(putState!=PutState.putIdle) {
            throw new RuntimeException("pvaClientMultiPut put already issued");
        }
        boolean allConnected = true;
        for(int i=0; i<nchannel; ++i) if(!channelPut[i].getChannel().isConnected()) allConnected = false;
        if(!allConnected) {
            badPut = true;
            return;
        }
        illegalPut = false;
        numPut = 0;
        numPutCallback = 0;
        for(int i=0; i<nchannel; ++i) {
            if(channelPut[i].getChannel().isConnected()) ++numPut;
        }
        badPut = false;
        putState = PutState.putActive;
        for(int i=0; i<nchannel; ++i) {
            if(!channelPut[i].getChannel().isConnected()) continue;
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
     * @return status of put request.
     */
    public Status waitPut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiPut was destroyed");
        checkConnected();
        if(illegalPut) {
            throw new RuntimeException("pvaClientMultiPut illegal put request");
        }
        try {
            lock.lock();
            try {
                if(putState==PutState.putActive) waitForPut.await();
            } catch(InterruptedException e) {
                return statusCreate.createStatus(
                        StatusType.ERROR,"pvaClientMultiPut waitPut exception " + e.getMessage(),
                        e.fillInStackTrace());
            }
        } finally {
            lock.unlock();
        }
        putState = PutState.putIdle;
        if(badPut) {
            return statusCreate.createStatus(StatusType.ERROR," put failed",null);
        }
        return statusCreate.getStatusOK();
    }
}
