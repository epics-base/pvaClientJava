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
    /**
     * Create a new PvaClientMultiGet.
     * @return The interface.
     */
    static PvaClientMultiGet create(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        PvaClientMultiGet multiGet = new PvaClientMultiGet(
                easyMultiChannel,channel,pvRequest,union);
        if(multiGet.init()) return multiGet;
        return null;
    }

    public PvaClientMultiGet(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        this.easyMultiChannel = easyMultiChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
        nchannel = channel.length;
        easyMultiData = easyMultiChannel.createEasyMultiData(pvRequest, union);

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
            return multiGet.getRequesterName();
        }
        public void message(String message, MessageType messageType) {
            multiGet.message(message, messageType);
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

    private final PvaClientMultiChannel easyMultiChannel;
    private final Channel[] channel;
    private final PVStructure pvRequest;
    private final int nchannel;
    private final PvaClientMultiData easyMultiData;

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
    private volatile boolean badGet = false;
    private enum GetState {getIdle,getActive,getFailed,getDone};
    private volatile GetState getState = GetState.getIdle;

    private volatile boolean isDestroyed = false;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForGet = lock.newCondition();
    private volatile Status status = statusCreate.getStatusOK();

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

    /**
     * Clean up
     */
     void destroy()
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
      * @return (false,true) if (not connected, connected)
      */
     boolean connect()
     {
         issueConnect();
         return waitConnect();
     }
     /**
      * create the channelGet for all channels.
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
         if(easyMultiChannel.allConnected()) {
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
      * @return (false,true) if (not all connected, all connected)
      */
     boolean waitConnect()
     {
         if(isDestroyed) return false;
         try {
             lock.lock();
             try {
                 if(numConnectCallback<numGetToConnect) waitForConnect.await();
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
         if(numGetConnected!=numGetToConnect) {
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
         issueGet();
         return waitGet();
     }

     /**
      * Issue a get for each channel.
      */
     void issueGet()
     {
         if(isDestroyed) return;
         checkConnected();
         if(getState!=GetState.getIdle) {
             Status status = statusCreate.createStatus(
                     StatusType.ERROR,"get already issued",null);
             setStatus(status);
             return;
         }
         boolean allConnected = true;
         for(int i=0; i<nchannel; ++i) if(!channelGet[i].getChannel().isConnected()) allConnected = false;
         if(!allConnected) {
             badGet = true;
             return;
         }
         numGet = 0;
         badGet = false;
         getState = GetState.getActive;
         easyMultiData.startDeltaTime();
         for(int i=0; i<nchannel; ++i){
             channelGet[i].get();
         }
     }
     /**
      * wait until all gets are complete.
      * @return (true,false) if (no errors, errors) resulted from gets.
      * If an error occurred then getStatus returns a reason.
      * @return (false,true) if (failure, success)
      */
     boolean waitGet()
     {
         if(isDestroyed) return false;
         checkConnected();
         try {
             lock.lock();
             try {
                 if(getState==GetState.getActive) waitForGet.await();
             } catch(InterruptedException e) {
                 Status status = statusCreate.createStatus(StatusType.ERROR, e.getMessage(), e.fillInStackTrace());
                 setStatus(status);
                 return false;
             }
         } finally {
             lock.unlock();
         }
         getState = GetState.getIdle;
         if(badGet) {
             Status status = statusCreate.createStatus(StatusType.ERROR," get failed",null);
             setStatus(status);
             return false;
         }
         easyMultiData.endDeltaTime();
         return true;
     }
     /**
      * Get the time when the last get was made.
      * @return The timeStamp.
      */
     TimeStamp getTimeStamp()
     {
         return easyMultiData.getTimeStamp();
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
         return easyMultiData.doubleOnly();
     }
     /**
      * Get the value field as a MTMultiChannel structure.
      * @return The value.
      * This is null if doubleOnly is true.
      */
     PVStructure getNTMultiChannel()
     {
         boolean result = checkGetState();
         if(!result) return null;
         return easyMultiData.getNTMultiChannel();
     }
     /**
      * Get the top level structure of the value field is a double[[]
      * @return The top level structure.
      * This is null if doubleOnly is false.
      */
     PVStructure getPVTop()
     {
         checkGetState();
         return easyMultiData.getPVTop();
     }
     /**
      * Return the value field.
      * @return The double[]
      * This is null if doubleOnly is false.
      */
     double[] getDoubleArray()
     {
         boolean result = checkGetState();
         if(!result) return null;
         return easyMultiData.getDoubleArray();
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
         boolean result = checkGetState();
         if(!result) return 0;
         return easyMultiData.getDoubleArray(offset, data, length);
     }
}
