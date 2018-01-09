/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.monitor.Monitor;
import org.epics.pvdata.monitor.MonitorElement;
import org.epics.pvdata.monitor.MonitorRequester;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;



/**
 * This is a synchronous alternative to channel monitor.
 * @author mrk
 * @since 2015.06
 */
public class PvaClientMonitor implements MonitorRequester{
    /**
     * Create a new PvaClientMonitor
     * @param pvaClient The single instance of pvaClient.
     * @param channel The pvaClientChannel.getChannel().
     * @param pvRequest The pvRequest
     * @return The new instance.
     */
    static PvaClientMonitor create(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        return new PvaClientMonitor(pvaClient,pvaClientChannel,pvRequest);
    }

    private PvaClientMonitor(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.pvaClientChannel = pvaClientChannel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::PvaClientMonitor"
                + " channelName " + pvaClientChannel.getChannelName());
        }
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final CreateRequest createRequest = new CreateRequest();

    private enum MonitorConnectState {connectIdle,connectWait,connectActive,connected};
    private final PvaClient pvaClient;
    private final PvaClientChannel pvaClientChannel;
    private PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForEvent = lock.newCondition();

    private PvaClientMonitorData pvaClientData = null;
    
    private volatile boolean isStarted = false;
    private volatile boolean isDestroyed = false;
    private volatile Status monitorConnectStatus = statusCreate.getStatusOK();
    private volatile Monitor monitor = null;
    private volatile MonitorElement monitorElement = null;
    
    private volatile PvaClientMonitorRequester pvaClientMonitorRequester = null;
    private volatile MonitorConnectState connectState = MonitorConnectState.connectIdle;
    private volatile boolean userPoll = false;
    private volatile boolean userWait = false;

    private void checkMonitorState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::checkMonitorState()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==MonitorConnectState.connectIdle)
        {
            connect();
            if(!isStarted) start();
            return;
        }
        if(connectState==MonitorConnectState.connectActive)
        {
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
                + " " + monitorConnectStatus.getMessage();
            throw new RuntimeException(message);
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#getRequesterName()
     */
    @Override
    public String getRequesterName() {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        return pvaClient.getRequesterName();
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#message(java.lang.String, org.epics.pvdata.pv.MessageType)
     */
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        pvaClient.message(message, messageType);
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.monitor.MonitorRequester#monitorConnect(org.epics.pvdata.pv.Status, org.epics.pvdata.monitor.Monitor, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void monitorConnect(
            Status status,
            Monitor monitor,
            Structure structure)
    {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::monitorConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            this.monitor = monitor;
            if(!status.isOK()) {
                 String message = "PvaClientMonitor::monitorConnect"
                   + "\npvRequest\n" + pvRequest
                   + "\nerror\n" + status.getMessage();
                 monitorConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,message,null);
                 return;
            }
        } finally {
            lock.unlock();
        }
        boolean signal = (connectState==MonitorConnectState.connectWait) ? true : false;
        monitorConnectStatus = status;
        connectState = MonitorConnectState.connected;
        if(isStarted) {
            System.out.println("PvaClientMonitor::monitorConnect()"
                    + " channelName " +  pvaClientChannel.getChannel().getChannelName()
                    + " is already started");
            return;
        }
        pvaClientData = PvaClientMonitorData.create(structure);
        pvaClientData.setMessagePrefix(pvaClientChannel.getChannel().getChannelName());
        if(signal) {
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientMonitor::monitorConnect() calling waitForConnect.signal");
            }
            lock.lock();
            try {
                waitForConnect.signal();
            } finally {
                lock.unlock();
            }
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientMonitor::monitorConnect() calling start");
            }
            start();
        } else {
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientMonitor::monitorConnect() calling start");
            }
            start();
        }
        if(pvaClientMonitorRequester!=null) pvaClientMonitorRequester.monitorConnect(status,this,structure);
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.monitor.MonitorRequester#monitorEvent(org.epics.pvdata.monitor.Monitor)
     */
    @Override
    public void monitorEvent(Monitor monitor) {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::monitorEvent()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(pvaClientMonitorRequester!=null) pvaClientMonitorRequester.event(this);
        lock.lock();
        try {
            if(userWait) waitForEvent.signal();
        } finally {
            lock.unlock();
        }
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.monitor.MonitorRequester#unlisten(org.epics.pvdata.monitor.Monitor)
     */
    @Override
    public void unlisten(Monitor monitor) {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::unlisten()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(pvaClientMonitorRequester!=null){
            pvaClientMonitorRequester.unlisten(this);
            return;
        }
        String message = "PvaClientMonitor::unlisten called but no requester to receive message";
        System.err.println(message);
    }

    /**
     * clean up resources used.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::destroy()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(monitor!=null) monitor.destroy();
        monitor = null;
        monitorElement = null;
    }
    /**
     * call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::connect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueConnect();
        Status status =  waitConnect();
        if(status.isOK()) return;
        String message =  "channel " + pvaClientChannel.getChannel().getChannelName()
        + " PvaClientMonitor::connect " + status.getMessage();
        throw new RuntimeException(message);
    }
    /**
     * create the monitor connection to the pvaClientChannel.getChannel().
     * This can only be called once.
     * @throws RuntimeException if failure.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::issueConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState!=MonitorConnectState.connectIdle) {
            String message =  "channel " + pvaClientChannel.getChannel().getChannelName() 
            + " pvaClientMonitor already connected" ;
            throw new RuntimeException(message);
        }
        connectState = MonitorConnectState.connectWait;
        monitor = pvaClientChannel.getChannel().createMonitor(this, pvRequest);
    }
    /**
     * wait until the monitor connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::waitConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(connectState==MonitorConnectState.connected) {
                if(!monitorConnectStatus.isOK()) connectState = MonitorConnectState.connectIdle;
                return monitorConnectStatus;
            }
            if(connectState!=MonitorConnectState.connectWait) {
                String message =  "channel " + pvaClientChannel.getChannel().getChannelName() 
                + " pvaClientMonitor illegal connect state ";
                throw new RuntimeException(message);
            }
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "pvaClientMonitor channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
        } finally {
            lock.unlock();
        }
        connectState = monitorConnectStatus.isOK()
                ? MonitorConnectState.connected : MonitorConnectState.connectIdle;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::waitConnect()"
                 + " monitorConnectStatus " +  monitorConnectStatus);
        }
        return monitorConnectStatus;
    }
    /**
     * Start monitoring.
     * This will wait until the monitor is connected.
     */
    public void start()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::start()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(isStarted) return;
        if(connectState==MonitorConnectState.connectIdle) connect();
        if(connectState!=MonitorConnectState.connected) {
            throw new RuntimeException("PvaClientMonitor::start illegal state");
        }
        isStarted = true;
        monitor.start();
    }
    /**
     * Start or restart the monitor with a new request.
     * @param request The new request.
     * This will wait until the monitor is connected.
     */
    public void start(String request)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::start(request)"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            throw new RuntimeException(createRequest.getMessage());
        }
        if(monitor!=null)
        {
            if(isStarted) monitor.stop();
            monitor.destroy();
            monitor = null;
        }
        isStarted = false;
        connectState = MonitorConnectState.connectIdle;
        userPoll = false;
        userWait = false;
        this.pvRequest = pvRequest;
        connect();
    }
    /**
     * Stop monitoring.
     */
    public void stop()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::stop()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(!isStarted) return;
        isStarted = false;
        monitor.stop();
    }
    /**
     * Is new monitor data available.
     * If true then getData can be called to get the data.
     * Also after done with data releaseEvent must be called before another call to poll or waitEvent.
     * @return true if monitor data is available or false if no new monitorElement are present.
     */
    public boolean poll()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::poll()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        checkMonitorState();
        if(!isStarted) {
            throw new RuntimeException("PvaClientMonitor::poll illegal state");
        }
        if(userPoll) {
            throw new RuntimeException("PvaClientMonitor::poll did not release last ");
        }
        monitorElement = monitor.poll();
        if(monitorElement==null) return false;
        userPoll = true;
        pvaClientData.setData(monitorElement);
        return true;
    }
    /**
     * Wait for a monitor event.
     * If true then getData can be called to get the data.
     * Also after done with data releaseEvent must be called before another call to poll or waitEvent.
     * @param secondsToWait Time to wait for event.
     * @return (false,true) means event (did not, did) occur.
     */
    public boolean waitEvent(double secondsToWait)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::waitEvent()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(!isStarted) {
            throw new RuntimeException("PvaClientMonitor::waitEvent illegal state");
        }
        lock.lock();
        try {
            if(poll()) return true;
            userWait = true;
            try {
                if(secondsToWait==0.0) {
                    waitForEvent.await();
                } else {
                    long nano = (long)(secondsToWait*1e9);
                    // No special action if timeout occurs.
                    waitForEvent.awaitNanos(nano);
                }
            }
            catch(InterruptedException e) {
                String message = "pvaClientMonitor::waitEvent channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            userWait = false;
            return poll();
        } finally {
            lock.unlock();
        }
    }
    /**
     * Release the monitorElement returned by poll.
     */
    public void releaseEvent()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::releaseEvent()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }

        if(!isStarted) {
            throw new RuntimeException("PvaClientMonitor::releaseEvent illegal state");
        }
        if(!userPoll) {
            throw new RuntimeException("PvaClientMonitor::releaseEvent did not call poll");
        }
        userPoll = false;
        monitor.release(monitorElement);
    }
    /**
     * Get the data in which monitor events are placed.
     * @return The interface.
     */
    public PvaClientMonitorData getData()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientMonitor::getData()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        checkMonitorState();
        return pvaClientData;
    }
    /**
     * Optional request to be notified when monitors occur.
     * @param requester The requester which must be implemented by the caller.
     */
    public void setRequester(PvaClientMonitorRequester requester)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::setRequester()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        pvaClientMonitorRequester = requester;
    }
    /**
     * Get the PvaClientChannel
     * @return The interface
     */
    public PvaClientChannel getPvaClientChannel()
    {
        return pvaClientChannel;
    }
}
