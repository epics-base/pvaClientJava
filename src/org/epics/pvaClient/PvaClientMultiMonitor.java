/**
 * 
 */
package org.epics.pvaClient;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.monitor.Monitor;
import org.epics.pvdata.monitor.MonitorElement;
import org.epics.pvdata.monitor.MonitorRequester;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
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
public class PvaClientMultiMonitor {

    /**
     * Create a new PvaClientMultiMonitor.
     * @return The interface.
     */
    static PvaClientMultiMonitor create(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        return new PvaClientMultiMonitor(easyMultiChannel,channel,pvRequest,union);
    }

    public PvaClientMultiMonitor(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        this.easyMultiChannel = easyMultiChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
        nchannel = channel.length;
        isMonitorConnected = new boolean[nchannel]
        monitorRequester = new MonitorRequesterPvt[nchannel];
        monitor = new Monitor[nchannel];
        monitorElement = new MonitorElement[nchannel];
        for(int i=0; i<nchannel; ++i) {
            isMonitorConnected[i] = false;
            monitorRequester[i] = new MonitorRequesterPvt(this,i);
            monitorElement[i] = null;
            if(channel[i].isConnected()) ++numMonitorToConnect;
        }
        easyMultiData = easyMultiChannel.createEasyMultiData(pvRequest, union);

    }
    
    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Status clientNotActive = statusCreate.createStatus(
            StatusType.ERROR,"method can only be called between poll and release",null);

    private static class MonitorRequesterPvt implements MonitorRequester
    {
        private final PvaClientMultiMonitor multiMonitor;
        private final int indChannel;

        MonitorRequesterPvt(PvaClientMultiMonitor xx, int indChannel)
        {
            this.multiMonitor = xx;
            this.indChannel = indChannel;
        }
        public String getRequesterName() {
            return multiMonitor.getRequesterName();
        }
        public void message(String message, MessageType messageType) {
            multiMonitor.message(message, messageType);
        }
        public void monitorConnect(Status status, Monitor monitor,
                Structure structure)
        {
            multiMonitor.monitorConnect(status,monitor,structure,indChannel);   
        }
        public void monitorEvent(Monitor monitor)
        {
            multiMonitor.monitorEvent(indChannel);
        }
        public void unlisten(Monitor monitor){
            multiMonitor.lostChannel(indChannel);
        }

    }
    private final PvaClientMultiChannel easyMultiChannel;
    private final Channel[] channel;
    private final PVStructure pvRequest;
    private final int nchannel;
    private final PvaClientMultiData easyMultiData;

    private MonitorRequesterPvt[] monitorRequester = null;
    private EasyMultiRequester multiRequester = null;
    private volatile Monitor[] monitor = null;
    private volatile MonitorElement[] monitorElement= null;

    // following used by connect
    private volatile int numMonitorToConnect = 0;
    private volatile int numConnectCallback = 0;
    private volatile int numMonitorConnected = 0;
    private volatile boolean[] isMonitorConnected = null;
    private enum ConnectState {connectIdle,connectActive,connectDone};
    private volatile ConnectState connectState = ConnectState.connectIdle;

    // following used by for monitors
    private int numMonitors = 0;
    private double waitBetweenEvents = 1.0;
    private enum MonitorState {monitorIdle,monitorActive,monitorClientActive};
    private volatile MonitorState monitorState = MonitorState.monitorIdle;
    private final TimeStamp timeStampBegin = TimeStampFactory.create();
    private final TimeStamp timeStampLatest = TimeStampFactory.create();

    private volatile boolean isDestroyed = false;
    private final ReentrantLock connectLock = new ReentrantLock();
    private final ReentrantLock monitorLock = new ReentrantLock();
    private final Condition waitForConnect = connectLock.newCondition();
    private volatile Status status = statusCreate.getStatusOK();


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
             if(monitor[i]!=null) monitor[i].destroy();
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
         connectState = ConnectState.connectActive;
         for(int i=0; i<channel.length; ++i) {
             if(channel[i].isConnected()) {
                 channel[i].createMonitor(monitorRequester[i], pvRequest);
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
             connectLock.lock();
             try {
                 if(numConnectCallback<numMonitorToConnect) waitForConnect.await();
             } catch(InterruptedException e) {
                 Status status = statusCreate.createStatus(
                         StatusType.ERROR,
                         e.getMessage(),
                         e.fillInStackTrace());
                 setStatus(status);
                 return false;
             }
         } finally {
             connectLock.unlock();
         }
         if(numMonitorConnected!=numMonitorToConnect) {
             Status status = statusCreate.createStatus(StatusType.ERROR," did not connect",null);
             setStatus(status);
             return false;
         }
         return true;
     }
     /**
      * Optional request to be notified when monitors occur.
      * @param requester The requester which must be implemented by the caller.
      */
     void setRequester(EasyMultiRequester requester)
     {
         multiRequester = requester;
     }

     /**
      * Start monitoring.
      * This will wait until the monitor is connected.
      * @param waitBetweenEvents The time to wait between events to see if more are available. 
      * @return (false,true) means (failure,success).
      * If false is returned then failure and getMessage will return reason.
      */
     boolean start(double waitBetweenEvents)
     {
         if(connectState==ConnectState.connectIdle) connect();
         if(connectState!=ConnectState.connectDone) {
             Status status = statusCreate.createStatus(StatusType.ERROR,"not connected",null);
             setStatus(status);
             return false;
         }
         if(monitorState!=MonitorState.monitorIdle) {
             Status status = statusCreate.createStatus(StatusType.ERROR,"not idle",null);
             setStatus(status);
             return false;
         }
         monitorState= MonitorState.monitorActive;
         this.waitBetweenEvents = waitBetweenEvents;
         timeStampBegin.getCurrentTime();
         easyMultiData.startDeltaTime();
         for(int i=0; i<nchannel; ++i) {
             if(isMonitorConnected[i]) monitor[i].start();
         }
         return true;
     }
     /**
      * Stop monitoring.
      */
     boolean stop()
     {
         if(monitorState!=MonitorState.monitorActive) {
             Status status = statusCreate.createStatus(StatusType.ERROR,"not active",null);
             setStatus(status);
             return false;
         }
         monitorState= MonitorState.monitorIdle;
         for(int i=0; i<nchannel; ++i) {
             if(isMonitorConnected[i]) monitor[i].stop();
         }
         return true;
     }
     /**
      * Get the monitorElements.
      * The client MUST only access the monitorElements between poll and release.
      * An element is null if it has data.
      * @return The monitorElements.
      */
     MonitorElement[] getMonitorElement()
     {
         return monitorElement;
     }
     /**
      * poll for new events.
      * @return the number of channels with a new monitor.
      * If > 0 then each element that is not null has the data for the corresponding channel.
      * A new poll can not be issued until release is called.
      */
     int poll()
     {
         monitorLock.lock();
         try {
             if(monitorState!=MonitorState.monitorActive) {
                 throw new IllegalStateException("monitor is not owner of elements");
             }
             int num = 0;
             monitorState = MonitorState.monitorClientActive;
             for(int i=0; i<nchannel; ++i) {
                 if(isMonitorConnected[i]) {
                     if(monitorElement[i]!=null) ++num;
                 }
             }
             if(num==0) monitorState = MonitorState.monitorActive;
             easyMultiData.endDeltaTime();
             return num;
         } finally {
             monitorLock.unlock();
         }
     }
     /**
      * Release each monitor element that is not null.
      * @return (false,true) if additional monitors are available.
      */
     boolean release()
     {
         monitorLock.lock();
         try {
             if(monitorState!=MonitorState.monitorClientActive) {
                 throw new IllegalStateException("user is not owner of elements");
             }
             boolean moreMonitors = false;

             for(int i=0; i < nchannel; ++i) {
                 if(monitorElement[i]!=null) {
                     monitor[i].release(monitorElement[i]);
                     monitorElement[i] = null;
                 }
             }
             easyMultiData.startDeltaTime();
             if(numMonitors>0) moreMonitors = true;
             numMonitors = 0;
             monitorState = MonitorState.monitorActive;
             return moreMonitors;
         } finally {
             monitorLock.unlock();
         }
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
         if(monitorState!=MonitorState.monitorClientActive) {
             setStatus(clientNotActive);
             return null;
         }
         return easyMultiData.getNTMultiChannel()
                 /**
                  * Get the top level structure of the value field is a double[[]
                  * @return The top level structure.
                  * This is null if doubleOnly is false.
                  */
                 PVStructure getPVTop()
         {
             if(monitorState!=MonitorState.monitorClientActive) {
                 setStatus(clientNotActive);
                 return null;
             }
             return easyMultiData.getPVTop();
         }
         /**
          * Return the value field.
          * @return The double[]
          * This is null if doubleOnly is false.
          */
         double[] getDoubleArray()
         {
             if(monitorState!=MonitorState.monitorClientActive) {
                 setStatus(clientNotActive);
                 return null;
             }
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
             if(monitorState!=MonitorState.monitorClientActive) {
                 setStatus(clientNotActive);
                 return 0;
             }
             return easyMultiData.getDoubleArray(offset, data, length);
         }
     }
}
