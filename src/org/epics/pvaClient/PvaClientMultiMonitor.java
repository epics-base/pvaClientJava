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
 * An easy way to monitor data from multiple channels.
 * @author mrk
 *
 */
public class PvaClientMultiMonitor {

    static PvaClientMultiMonitor create(
            PvaClientMultiChannel pvaClientMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        return new PvaClientMultiMonitor(pvaClientMultiChannel,channel,pvRequest,union);
    }

    private PvaClientMultiMonitor(
            PvaClientMultiChannel pvaClientMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
        nchannel = channel.length;
        isMonitorConnected = new boolean[nchannel];
        monitorRequester = new MonitorRequesterPvt[nchannel];
        monitor = new Monitor[nchannel];
        monitorElement = new MonitorElement[nchannel];
        for(int i=0; i<nchannel; ++i) {
            isMonitorConnected[i] = false;
            monitorRequester[i] = new MonitorRequesterPvt(this,i);
            monitorElement[i] = null;
            if(channel[i].isConnected()) ++numMonitorToConnect;
        }
        pvaClientMultiData = PvaClientMultiData.create(pvaClientMultiChannel, channel, pvRequest, union);

    }
    
    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
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
            return multiMonitor.pvaClientMultiChannel.getPvaClient().getRequesterName();
        }
        public void message(String message, MessageType messageType) {
            multiMonitor.pvaClientMultiChannel.getPvaClient().message(message, messageType);
        }
        public void monitorConnect(
            Status status,
            Monitor monitor,
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
    
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final Channel[] channel;
    private final PVStructure pvRequest;
    private final int nchannel;
    private final PvaClientMultiData pvaClientMultiData;

    private MonitorRequesterPvt[] monitorRequester = null;
    private PvaClientMultiMonitorRequester multiRequester = null;
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
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
 
    private void monitorConnect(
            Status status,
            Monitor monitor,
            Structure structure,
            int index)
    {
        if(isDestroyed) return;
        this.monitor[index] = monitor;
        if(status.isOK()) {  
            if(!isMonitorConnected[index]) {
                ++numMonitorConnected;
                isMonitorConnected[index] = true;
                pvaClientMultiData.setStructure(structure, index);
            }
        } else {
            if(isMonitorConnected[index]) {
                --numMonitorConnected;
                isMonitorConnected[index] = false;
                pvaClientMultiChannel.getPvaClient().message(status.getMessage(), MessageType.error);
            }
        }
        if(connectState!=ConnectState.connectActive) return;
        lock.lock();
        try {
            numConnectCallback++;
            if(numConnectCallback==numMonitorToConnect) {
                connectState = ConnectState.connectDone;
                waitForConnect.signal();
            }
        } finally {
            lock.unlock();
        }
    }
    
    private void monitorEvent(int index)
    {
        if(isDestroyed) return;
        boolean callRequester = false;
        lock.lock();
        try {
            ++numMonitors;
            if(numMonitors==nchannel) callRequester=true;
            if(monitorState!=MonitorState.monitorActive) return;
            if(monitorElement[index]!=null) return;
            monitorElement[index] = monitor[index].poll();
            MonitorElement element = monitorElement[index];
            if(element!=null) {
                pvaClientMultiData.setPVStructure(
                        element.getPVStructure(),element.getChangedBitSet(),index);
            }
            timeStampLatest.getCurrentTime();
            double diff = timeStampLatest.diff(timeStampLatest, timeStampBegin);
            if(diff>=waitBetweenEvents) callRequester = true;
        } finally {
            lock.unlock();
        }
        if(callRequester&&(multiRequester!=null)) multiRequester.event(this);
    }
    
    private void lostChannel(int index)
    {
        lock.lock();
        try {
        isMonitorConnected[index] = false;
        monitor[index] = null;
        monitorElement[index] = null;
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
             if(monitor[i]!=null) monitor[i].destroy();
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
      * create the channel monitor for all channels.
      */
     public void issueConnect()
     {
         if(isDestroyed) return;
         if(connectState!=ConnectState.connectIdle) {
             throw new RuntimeException("pvaClientMultiMonitor connect already issued");
         }
         numConnectCallback = 0;
         if(pvaClientMultiChannel.allConnected()) {
              numMonitorToConnect = channel.length;
         } else {
             for(int i=0; i<channel.length; ++i) {
                 if(channel[i].isConnected()) numMonitorToConnect++;
             }
         }
         connectState = ConnectState.connectActive;
         for(int i=0; i<channel.length; ++i) {
             if(channel[i].isConnected()) {
                 channel[i].createMonitor(monitorRequester[i], pvRequest);
             }
         }
     }
     /**
      * Wait until all channel monitors are created.
      * @return status of connection request.
      */
     public Status waitConnect()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiGet was destroyed");
         try {
             lock.lock();
             try {
                 if(numConnectCallback<numMonitorToConnect) waitForConnect.await();
             } catch(InterruptedException e) {
                 return statusCreate.createStatus(
                     StatusType.ERROR,"pvaClientMultiMonitor waitConnect exception " + e.getMessage(),
                     e.fillInStackTrace());
             }
         } finally {
             lock.unlock();
         }
         if(numMonitorConnected!=numMonitorToConnect) {
             String message = "pvaClientMultiMonitor waitConnect";
             message += " numMonitorConnected " + numMonitorConnected;
             message += " but numMonitorToConnect "  + numMonitorToConnect;
             return statusCreate.createStatus(StatusType.ERROR, message,null);
         }
         return statusCreate.getStatusOK();
     }
     /**
      * Optional request to be notified when monitors occur.
      * @param requester The requester which must be implemented by the caller.
      */
     public void setRequester(PvaClientMultiMonitorRequester requester)
     {
         multiRequester = requester;
     }

     /**
      * Start monitoring.
      * This will wait until the monitor is connected.
      * If any problem is encountered an exception is thrown.
      * @param waitBetweenEvents The time to wait between events to see if more are available. 
      */
     public void start(double waitBetweenEvents)
     {
         if(connectState==ConnectState.connectIdle) connect();
         if(connectState!=ConnectState.connectDone) {
             throw new RuntimeException("pvaClientMultiMonitor::start not connected");
         }
         if(monitorState!=MonitorState.monitorIdle) {
             throw new RuntimeException("pvaClientMultiMonitor::start not idle");
         }
         monitorState= MonitorState.monitorActive;
         this.waitBetweenEvents = waitBetweenEvents;
         timeStampBegin.getCurrentTime();
         pvaClientMultiData.startDeltaTime();
         for(int i=0; i<nchannel; ++i) {
             if(isMonitorConnected[i]) monitor[i].start();
         }
     }
     /**
      * Stop monitoring.
      * If any problem is encountered an exception is thrown.
      */
     public void stop()
     {
         if(monitorState!=MonitorState.monitorActive) {
             throw new RuntimeException("pvaClientMultiMonitor::start not active");
         }
         monitorState= MonitorState.monitorIdle;
         for(int i=0; i<nchannel; ++i) {
             if(isMonitorConnected[i]) monitor[i].stop();
         }
     }
     /**
      * Get the monitorElements.
      * The client MUST only access the monitorElements between poll and release.
      * An element is null if it has data.
      * @return The monitorElements.
      */
     public MonitorElement[] getMonitorElement()
     {
         return monitorElement;
     }
     /**
      * poll for new events.
      * @return the number of channels with a new monitor.
      * If > 0 then each element that is not null has the data for the corresponding channel.
      * A new poll can not be issued until release is called.
      */
     public int poll()
     {
         lock.lock();
         try {
             if(monitorState!=MonitorState.monitorActive) {
                 throw new IllegalStateException("pvaClientMultiMonitor::poll monitor is not owner of elements");
             }
             int num = 0;
             monitorState = MonitorState.monitorClientActive;
             for(int i=0; i<nchannel; ++i) {
                 if(isMonitorConnected[i]) {
                     if(monitorElement[i]!=null) ++num;
                 }
             }
             if(num==0) monitorState = MonitorState.monitorActive;
             pvaClientMultiData.endDeltaTime();
             return num;
         } finally {
             lock.unlock();
         }
     }
     /**
      * Release each monitor element that is not null.
      * @return (false,true) if additional monitors are available.
      */
     public boolean release()
     {
         lock.lock();
         try {
             if(monitorState!=MonitorState.monitorClientActive) {
                 throw new IllegalStateException("pvaClientMultiMonitor::release user is not owner of elements");
             }
             boolean moreMonitors = false;

             for(int i=0; i < nchannel; ++i) {
                 if(monitorElement[i]!=null) {
                     monitor[i].release(monitorElement[i]);
                     monitorElement[i] = null;
                 }
             }
             pvaClientMultiData.startDeltaTime();
             if(numMonitors>0) moreMonitors = true;
             numMonitors = 0;
             monitorState = MonitorState.monitorActive;
             return moreMonitors;
         } finally {
             lock.unlock();
         }
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
      * An exception is thrown if client is not active.
      * @return The value.
      */
     public PVStructure getNTMultiChannel()
     {
         if(monitorState!=MonitorState.monitorClientActive) {
             throw new IllegalStateException("pvaClientMultiMonitor::getNTMultiChannel client is not active");
         }
         return pvaClientMultiData.getNTMultiChannel();
     }
     /**
      * Get the top level structure for the data,
      * An exception is thrown if client is not active.
      * @return The top level structure.
      */
     public PVStructure getPVTop()
     {
         if(monitorState!=MonitorState.monitorClientActive) {
             throw new IllegalStateException("pvaClientMultiMonitor::getNTMultiChannel client is not active");
         }
         return pvaClientMultiData.getPVTop();
     }
     /**
      * Return the value field.
      * An exception is thrown if client is not active.
      * @return The double[]
      */
     public double[] getDoubleArray()
     {
         if(monitorState!=MonitorState.monitorClientActive) {
             throw new IllegalStateException("pvaClientMultiMonitor::getNTMultiChannel client is not active");
         }
         return pvaClientMultiData.getDoubleArray();
     }
     /**
      * Get the data from the value field.
      * An exception is thrown if client is not active.
      * @param offset The offset into the data of the value field.
      * @param data The place to copy the data.
      * @param length The number of elements to copy.
      * @return The number of elements copied.
      */
     public int getDoubleArray(int offset, double[]data,int length)
     {
         if(monitorState!=MonitorState.monitorClientActive) {
             throw new IllegalStateException("pvaClientMultiMonitor::getNTMultiChannel client is not active");
             
         }
         return pvaClientMultiData.getDoubleArray(offset, data, length);
     }
}
