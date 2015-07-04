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
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;



/**
 * EasyMonitor is not implemented.
 * The following is a guess at the methods to be implemented.
 * @author mrk
 *
 */
public class PvaClientMonitor implements MonitorRequester{
    /**
     * Create a new PvaClientMonitor
     * @return The interface.
     */
    static PvaClientMonitor create(
            PvaClientChannel pvaClientChannel,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientMonitor(pvaClientChannel,channel,pvRequest);
    }

    PvaClientMonitor(
            PvaClientChannel pvaClientChannel,
            Channel channel,
            PVStructure pvRequest)
            {
        this.pvaClientChannel = pvaClientChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
            }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum MonitorConnectState {connectIdle,connectActive,connected,monitorStarted};
    private final PvaClientChannel pvaClientChannel;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForEvent = lock.newCondition();

    private PvaClientMonitorData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status connectStatus = statusCreate.getStatusOK();
    private volatile Monitor monitor = null;
    private volatile MonitorElement monitorElement = null;
    private volatile PvaClientMonitorRequester monitorRequester = null;

    private volatile MonitorConnectState connectState = MonitorConnectState.connectIdle;
    private volatile boolean userPoll = false;
    private volatile boolean userWait = false;

    void checkMonitorState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState==MonitorConnectState.connectIdle) connect();
        if(connectState==MonitorConnectState.connected) start();
    }

    @Override
    public String getRequesterName() {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        return pvaClientChannel.getChannelName();
    }
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        pvaClientChannel.message(message, messageType);
    }
    @Override
    public void monitorConnect(
            Status status,
            Monitor monitor,
            Structure structure)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        connectStatus = status;
        this.monitor = monitor;
        if(!status.isSuccess()) {
            pvaClientData = PvaClientMonitorData.create(structure);
            pvaClientData.setMessagePrefix(channel.getChannelName());
        }
        lock.lock();
        try {
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public void monitorEvent(Monitor monitor) {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(monitorRequester!=null) monitorRequester.event(this);
        if(!userWait) return;
        lock.lock();
        try {
            waitForEvent.signal();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public void unlisten(Monitor monitor) {
        destroy();
    }

    /**
     * clean up resources used.
     */
    void destroy()
    {
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
     * @return the result from waitConnect.
     */
    void connect()
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
     */
    void issueConnect()
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
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState!=MonitorConnectState.connectActive) {
            String message =  "channel " + channel.getChannelName() 
                 + " pvaClientMonitor illegal connect state ";
            throw new RuntimeException(message);
        }
        try {
            lock.lock();
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "pvaClientMonitor channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
        } finally {
            lock.unlock();
        }
        if(connectStatus.isOK()){
            connectState = MonitorConnectState.connected;
            return statusCreate.getStatusOK();
        }
        connectState = MonitorConnectState.connectIdle;
        return connectStatus;
    }

    /**
     * Optional request to be notified when monitors occur.
     * @param requester The requester which must be implemented by the caller.
     */
    void setRequester(PvaClientMonitorRequester requester)
    {
        monitorRequester = requester;
    }

    /**
     * Start monitoring.
     * This will wait until the monitor is connected.
     * If false is returned then failure and getNessage will return reason.
     */
    void start()
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
    void stop()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState!=MonitorConnectState.monitorStarted) return;
        connectState = MonitorConnectState.connected;
        monitor.stop();
    }
    /**
     * Get the data for the next monitor.
     * @return the next monitor or null if no new monitorElement are present.
     * If successful releaseEvent must be called before another call to poll.
     */
    boolean poll()
    {
        checkMonitorState();
        if(connectState!=MonitorConnectState.monitorStarted) {
            throw new RuntimeException("PvaClientMonitor::poll illegal state");
        }
        if(userPoll) {
            throw new RuntimeException("PvaClientMonitor::polldid not release last ");
        }
        monitorElement = monitor.poll();
        if(monitorElement==null) return false;
        userPoll = true;
        pvaClientData.setData(monitorElement);
        return true;
    }
    /**
     * Wait for a monitor event.
     * The data will be in PvaClientData.
     * @param secondsToWait Time to wait for event.
     * @return (false,true) means event (did not, did) occur.
     */
    boolean waitEvent(double secondsToWait)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState!=MonitorConnectState.monitorStarted) {
            throw new RuntimeException("PvaClientMonitor::poll illegal state");
        }
        if(poll()) return true;
        userWait = true;
        try {
            lock.lock();
            try {
                long nano = (long)(secondsToWait*1e9);
                waitForEvent.awaitNanos(nano);
            } catch(InterruptedException e) {
                String message = "pvaClientMonitor::waitEvent channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
        } finally {
            lock.unlock();
        }
        userWait = false;
        return poll();
    }
    /**
     * Release the monitorElement returned by poll.
     */
    void releaseEvent()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMonitor was destroyed");
        if(connectState!=MonitorConnectState.monitorStarted) {
            throw new RuntimeException("PvaClientMonitor::poll illegal state");
        }
        if(!userPoll) {
            throw new RuntimeException("PvaClientMonitor::poll did not call poll");
        }
        userPoll = false;
        monitor.release(monitorElement);
    }
    /**
     * Get the data in which monitor events are placed.
     * @return The interface.
     */
    PvaClientMonitorData getData()
    {
        checkMonitorState();
        return pvaClientData;
    }  
}
