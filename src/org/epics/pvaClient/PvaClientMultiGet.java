/**
 * 
 */
package org.epics.pvaClient;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.ChannelGet;
import org.epics.pvaccess.client.ChannelGetRequester;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.Union;


/**
 * An easy way to get data from multiple channels.
 * @author mrk
 *
 */
public class PvaClientMultiGet {
    static PvaClientMultiGet create(
            PvaClientMultiChannel pvaClientMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        PvaClientMultiGet multiGet = new PvaClientMultiGet(
                pvaClientMultiChannel,channel,pvRequest,union);
        if(multiGet.init()) return multiGet;
        return null;
    }

    private PvaClientMultiGet(
            PvaClientMultiChannel pvaClientMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
        nchannel = channel.length;
        pvaClientMultiData = PvaClientMultiData.create(pvaClientMultiChannel, channel, pvRequest, union);

    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private static class ChannelGetRequesterPvt implements ChannelGetRequester
    {
        private final PvaClientMultiGet multiGet;
        private final int indChannel;

        ChannelGetRequesterPvt(PvaClientMultiGet multiGet,int indChannel)
        {
            this.multiGet = multiGet;
            this.indChannel = indChannel;
        }
        public String getRequesterName() {
            return multiGet.pvaClientMultiChannel.getPvaClient().getRequesterName();
        }
        public void message(String message, MessageType messageType) {
            multiGet.pvaClientMultiChannel.getPvaClient().message(message, messageType);
        }
        public void channelGetConnect(Status status, ChannelGet channelGet,
                Structure structure)
        {
            multiGet.channelGetConnect(status,channelGet,structure,indChannel);
        }
        public void getDone(Status status, ChannelGet channelGet,
                PVStructure pvStructure, BitSet bitSet)
        {
            multiGet.getDone(status,channelGet,pvStructure,bitSet,indChannel);
        }
    }

    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final Channel[] channel;
    private final PVStructure pvRequest;
    private final int nchannel;
    private final PvaClientMultiData pvaClientMultiData;

    private ChannelGetRequesterPvt[] channelGetRequester = null;
    private volatile ChannelGet[] channelGet = null;



    // following used by connect
    private volatile int numGetToConnect = 0;
    private volatile int numConnectCallback = 0;
    private volatile int numGetConnected = 0;
    private volatile boolean[] isGetConnected = null;
    private enum ConnectState {connectIdle,connectActive,connectDone};
    private volatile ConnectState connectState = ConnectState.connectIdle;

    // following used by get
    private volatile int numGet = 0;
    private volatile int numGetCallback = 0;
    private volatile boolean badGet = false;
    private enum GetState {getIdle,getActive,getFailed,getDone};
    private volatile GetState getState = GetState.getIdle;

    private volatile boolean isDestroyed = false;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForGet = lock.newCondition();

    private boolean init()
    {
        channelGetRequester = new ChannelGetRequesterPvt[nchannel];
        isGetConnected = new boolean[nchannel];
        channelGet = new ChannelGet[nchannel];
        for(int i=0; i<nchannel; ++i) {
            channelGetRequester[i] = new ChannelGetRequesterPvt(this,i);
            isGetConnected[i] = false;
            channelGet[i] = null;
            if(channel[i].isConnected()) ++numGetToConnect;
        }
        return true;
    }
    
    private void checkConnected()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
        if(connectState==ConnectState.connectIdle) connect();
    }

    private void channelGetConnect(
            Status status,
            ChannelGet channelGet,
            Structure structure,
            int index)
    {
        if(isDestroyed) return;
        this.channelGet[index] = channelGet;
        if(status.isOK()) {  
            if(!isGetConnected[index]) {
                ++numGetConnected;
                isGetConnected[index] = true;
                pvaClientMultiData.setStructure(structure, index);
            }
        } else {
            if(isGetConnected[index]) {
                --numGetConnected;
                isGetConnected[index] = false;
                pvaClientMultiChannel.getPvaClient().message(status.getMessage(), MessageType.error);
            }
        }
        if(connectState!=ConnectState.connectActive) return;
        lock.lock();
        try {
            numConnectCallback++;
            if(numConnectCallback==numGetToConnect) {
                connectState = ConnectState.connectDone;
                waitForConnect.signal();
            }
        } finally {
            lock.unlock();
        }
    }
    private void getDone(
            Status status,
            ChannelGet channelGet,
            PVStructure pvStructure,
            BitSet bitSet,
            int index)
    {
        if(isDestroyed) return;
        if(status.isOK()) {
            pvaClientMultiData.setPVStructure(pvStructure, bitSet, index);
        } else {
            badGet = true;
            pvaClientMultiChannel.getPvaClient().message(
                    "getDone " + channel[index].getChannelName() + " " +status.getMessage(),
                    MessageType.error);
        }
        lock.lock();
        try {
            ++numGetCallback;
            if(numGetCallback==numGet) {
                if(badGet) {
                    getState = GetState.getFailed;
                } else {
                    getState = GetState.getDone;
                }
                waitForGet.signal();
            }
        } finally {
            lock.unlock();
        }
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
        for(int i=0; i<nchannel; ++i){
            if(channelGet[i]!=null) channelGet[i].destroy();
        }
    }
     /**
      * Calls issueConnect and then waitConnect.
      * If connection is not made an exception is thrown.
      */
     public void connect()
     {
         issueConnect();
         Status status =  waitConnect();
         if(!status.isOK()) throw new RuntimeException(status.getMessage());
     }
     /**
      * create the channelGet for all channels.
      */
     public void issueConnect()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         if(connectState!=ConnectState.connectIdle) {
             throw new RuntimeException("pvaClientMultiGet connect already issued");
         }
         numConnectCallback = 0;
         if(pvaClientMultiChannel.allConnected()) {
             numGetToConnect = channel.length;
         } else {
             for(int i=0; i<channel.length; ++i) {
                 if(channel[i].isConnected()) numGetToConnect++;
             }
         }
         connectState = ConnectState.connectActive;
         for(int i=0; i<channel.length; ++i) {
             if(channel[i].isConnected()) {
                 channel[i].createChannelGet(channelGetRequester[i], pvRequest);
             }
         }
     }
     /**
      * Wait until all channelGets are created.
      * @return status of connection request.
      */
     public Status waitConnect()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         try {
             lock.lock();
             try {
                 if(numConnectCallback<numGetToConnect) waitForConnect.await();
             } catch(InterruptedException e) {
                 return statusCreate.createStatus(
                     StatusType.ERROR,"pvaClientMultiGet waitConnect exception " + e.getMessage(),
                     e.fillInStackTrace());
             }
         } finally {
             lock.unlock();
         }
         if(numGetConnected!=numGetToConnect) {
             String message = "pvaClientMultiGet waitConnect";
             message += " numGetConnected " + numGetConnected;
             message += " but numGetToConnect "  + numGetToConnect;
             return statusCreate.createStatus(StatusType.ERROR, message,null);
         }
         return statusCreate.getStatusOK();
     }
     /**
      * Call issueGet and then waitGet.
      * An exception is thrown if get fails
      * 
      */
     public void get()
     {
         issueGet();
         Status status =  waitGet();
         if(!status.isOK()) throw new RuntimeException("pvaClientMultiGet::get " +status.getMessage());
     }

     /**
      * Issue a get for each channel.
      */
     public void issueGet()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         checkConnected();
         if(getState!=GetState.getIdle) {
             throw new RuntimeException("pvaClientMultiGet::issueGet get already issued");
         }
         numGet = 0;
         numGetCallback = 0;
         for(int i=0; i<nchannel; ++i) {
             if(channelGet[i].getChannel().isConnected()) ++numGet;
         }
         getState = GetState.getActive;
         pvaClientMultiData.startDeltaTime();
         for(int i=0; i<nchannel; ++i){
             if(channelGet[i].getChannel().isConnected()) channelGet[i].get();
         }
     }
     /**
      * wait until all gets are complete.
      * @return status of get request.
      */
     public Status waitGet()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         checkConnected();
         try {
             lock.lock();
             try {
                 if(getState==GetState.getActive) waitForGet.await();
             } catch(InterruptedException e) {
                 return statusCreate.createStatus(
                         StatusType.ERROR,"pvaClientMultiGet waitGet exception " + e.getMessage(),
                         e.fillInStackTrace());
             }
         } finally {
             lock.unlock();
         }
         getState = GetState.getIdle;
         if(badGet) {
             return statusCreate.createStatus(StatusType.ERROR,"pvaClientMultiGet waitGet get failed",null);
         }
         pvaClientMultiData.endDeltaTime();
         return statusCreate.getStatusOK();
     }
     /**
      * Get the time when the last get was made.
      * @return The timeStamp.
      */
     public TimeStamp getTimeStamp()
     {
         return pvaClientMultiData.getTimeStamp();
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
         return pvaClientMultiData.doubleOnly();
     }
     /**
      * Get the value field as a MTMultiChannel structure.
      * @return The value.
      */
     public PVStructure getNTMultiChannel()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         return pvaClientMultiData.getNTMultiChannel();
     }
     /**
      * Get the top level structure of the value field is a double[[]
      * @return The top level structure.
      */
     public PVStructure getPVTop()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         return pvaClientMultiData.getPVTop();
     }
     /**
      * Return the value field.
      * @return The double[]
      * This is null if doubleOnly is false.
      */
     public double[] getDoubleArray()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         return pvaClientMultiData.getDoubleArray();
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
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         return pvaClientMultiData.getDoubleArray(offset, data, length);
     }
}
