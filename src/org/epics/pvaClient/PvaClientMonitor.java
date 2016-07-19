/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
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
     * @param channel The channel.
     * @param pvRequest The pvRequest
     * @return The new instance.
     */
    static PvaClientMonitor create(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientMonitor(pvaClient,channel,pvRequest);
    }

    private PvaClientMonitor(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.channel = channel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) System.out.println("PvaClientMonitor::PvaClientMonitor");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum MonitorConnectState {connectIdle,connectActive,connected,monitorStarted};
    private final PvaClient pvaClient;
    private final Channel channel;
    private final PVStructure pvRequest;
    private volatile PvaClientMonitorRequester monitorRequester = null;
    private volatile PvaClientUnlistenRequester unlistenRequester = null;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForEvent = lock.newCondition();

    private PvaClientMonitorData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status connectStatus = statusCreate.getStatusOK();
    private volatile Monitor monitor = null;
    private volatile MonitorElement monitorElement = null;


    private volatile MonitorConnectState connectState = MonitorConnectState.connectIdle;
    private volatile boolean userPoll = false;
    private volatile boolean userWait = false;

    private void checkMonitorState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState==MonitorConnectState.connectIdle) connect();
        if(connectState==MonitorConnectState.connected) start();
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
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        lock.lock();
        try {
            connectStatus = status;
            connectState = MonitorConnectState.connected;
            this.monitor = monitor;
            if(status.isSuccess()) {
                pvaClientData = PvaClientMonitorData.create(structure);
                pvaClientData.setMessagePrefix(channel.getChannelName());
            }
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.monitor.MonitorRequester#monitorEvent(org.epics.pvdata.monitor.Monitor)
     */
    @Override
    public void monitorEvent(Monitor monitor) {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        lock.lock();
        try {
            if(PvaClient.getDebug()) System.out.println("PvaClientMonitor::monitorEvent");
            if(monitorRequester!=null) monitorRequester.event(this);
            if(!userWait) return;
            waitForEvent.signal();
        } finally {
            lock.unlock();
        }
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.monitor.MonitorRequester#unlisten(org.epics.pvdata.monitor.Monitor)
     */
    @Override
    public void unlisten(Monitor monitor) {
        if(PvaClient.getDebug()) System.out.println("PvaClientMonitor::unlisten");
        if(unlistenRequester!=null) {
            unlistenRequester.unlisten(this);
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
        if(PvaClient.getDebug()) System.out.println("PvaClientMonitor::destroy");
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
        issueConnect();
        Status status =  waitConnect();
        if(status.isOK()) return;
        String message =  "channel " + channel.getChannelName()
        + " PvaClientMonitor::connect " + status.getMessage();
        throw new RuntimeException(message);
    }
    /**
     * create the monitor connection to the channel.
     * This can only be called once.
     * @throws RuntimeException if failure.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState!=MonitorConnectState.connectIdle) {
            String message =  "channel " + channel.getChannelName() 
            + " pvaClientMonitor already connected" ;
            throw new RuntimeException(message);
        }
        connectState = MonitorConnectState.connectActive;
        monitor = channel.createMonitor(this, pvRequest);
    }
    /**
     * wait until the monitor connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        lock.lock();
        try {
            if(connectState==MonitorConnectState.connected) {
                if(!connectStatus.isOK()) connectState = MonitorConnectState.connectIdle;
                return connectStatus;
            }
            if(connectState!=MonitorConnectState.connectActive) {
                String message =  "channel " + channel.getChannelName() 
                + " pvaClientMonitor illegal connect state ";
                throw new RuntimeException(message);
            }
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "pvaClientMonitor channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            if(!connectStatus.isOK()) connectState = MonitorConnectState.connectIdle;
            return connectStatus;
        } finally {
            lock.unlock();
        }
    }
    /**
     * Optional request to be notified when monitors occur.
     * @param requester The requester which must be implemented by the caller.
     */
    public void setRequester(PvaClientMonitorRequester requester)
    {
        monitorRequester = requester;
    }
    /**
     * Optional request to be notified when unlisten occur.
     * @param requester The requester which must be implemented by the caller.
     */
    public void setUnlistenRequester(PvaClientUnlistenRequester requester)
    {
        unlistenRequester = requester;
    }
    /**
     * Start monitoring.
     * This will wait until the monitor is connected.
     */
    public void start()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState==MonitorConnectState.monitorStarted) return;
        if(connectState==MonitorConnectState.connectIdle) connect();
        if(connectState!=MonitorConnectState.connected) {
            throw new RuntimeException("PvaClientMonitor::start illegal state");
        }
        connectState = MonitorConnectState.monitorStarted;
        monitor.start();
    }
    /**
     * Stop monitoring.
     */
    public void stop()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState!=MonitorConnectState.monitorStarted) return;
        connectState = MonitorConnectState.connected;
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
        checkMonitorState();
        if(connectState!=MonitorConnectState.monitorStarted) {
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
        if(connectState!=MonitorConnectState.monitorStarted) {
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
                    waitForEvent.awaitNanos(nano);
                }
            }
            catch(InterruptedException e) {
                String message = "pvaClientMonitor::waitEvent channel "
                        + channel.getChannelName() 
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
        if(connectState!=MonitorConnectState.monitorStarted) {
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
        checkMonitorState();
        return pvaClientData;
    }  
}
