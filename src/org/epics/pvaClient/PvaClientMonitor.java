/**
 * 
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvdata.property.*;
import org.epics.pvdata.factory.*;
import org.epics.pvdata.pv.*;
import org.epics.pvdata.misc.*;
import org.epics.pvdata.monitor.*;

import org.epics.pvaccess.client.*;



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
            PvaClientChannel easyChannel,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientMonitor(easyChannel,channel,pvRequest);
    }

    PvaClientMonitor(
            PvaClientChannel easyChannel,
            Channel channel,
            PVStructure pvRequest)
            {
        this.easyChannel = easyChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
            }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private final PvaClientChannel easyChannel;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();

    private volatile boolean isDestroyed = false;
    private volatile Status status = statusCreate.getStatusOK();
    private volatile Monitor monitor = null;
    private volatile MonitorElement monitorElement = null;
    private volatile PvaClientMonitorRequester easyRequester = null;

    private enum ConnectState {connectIdle,notConnected,connected};
    private volatile ConnectState connectState = ConnectState.connectIdle;
    private volatile boolean waitingForConnect = false;

    private boolean checkConnected() {
        if(connectState==ConnectState.connectIdle) connect();
        if(connectState==ConnectState.connected) return true;
        if(connectState==ConnectState.notConnected) {
            String message = channel.getChannelName() + " monitor not connected";
            Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
            setStatus(status);
            return false;
        }
        String message = channel.getChannelName() + " illegal monitor state";
        Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
        setStatus(status);
        return false;

    }

    private volatile boolean isStarted = false;

    @Override
    public String getRequesterName() {
        return easyChannel.getChannelName();
    }
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) return;
        easyChannel.message(message, messageType);
    }
    @Override
    public void monitorConnect(
            Status status,
            Monitor monitor,
            Structure structure)
    {
        if(isDestroyed) return;
        this.monitor = monitor;
        if(!status.isSuccess()) {
            setStatus(status);
            connectState = ConnectState.notConnected;
        } else {
            connectState = ConnectState.connected;
        }
        lock.lock();
        try {
            if(waitingForConnect) waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public void monitorEvent(Monitor monitor) {
        if(isDestroyed) return;
        if(easyRequester!=null) easyRequester.event(this);
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
        monitorElement = null;
    }
    /**
     * call issueConnect and then waitConnect.
     * @return the result from waitConnect.
     */
    boolean connect()
    {
        issueConnect();
        return waitConnect();
    }
    /**
     * create the monitor connection to the channel.
     * This can only be called once.
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
        monitor = channel.createMonitor(this, pvRequest);
    }
    /**
     * wait until the monitor connection to the channel is complete.
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    boolean waitConnect()
    {
        if(isDestroyed) return false;
        try {
            lock.lock();
            try {
                waitingForConnect = true;
                if(connectState==ConnectState.connectIdle) waitForConnect.await();
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
        if(connectState==ConnectState.notConnected) return false;
        return true;
    }

    /**
     * Optional request to be notified when monitors occur.
     * @param requester The requester which must be implemented by the caller.
     */
    void setRequester(PvaClientMonitorRequester requester)
    {
        easyRequester = requester;
    }

    /**
     * Start monitoring.
     * This will wait until the monitor is connected.
     * If false is returned then failure and getNessage will return reason.
     */
    boolean start()
    {
        if(!checkConnected()) return false;
        if(isStarted) {
            String message = channel.getChannelName() + "already started";
            Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
            setStatus(status);
            return false;
        }
        isStarted = true;
        monitorElement = null;
        monitor.start();
        return true;
    }
    /**
     * Stop monitoring.
     */
    boolean stop()
    {
        if(!checkConnected()) return false;
        if(isStarted) {
            String message = channel.getChannelName() + "not started";
            Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
            setStatus(status);
            return false;
        }
        isStarted = false;
        monitor.stop();
        monitorElement = null;
        return true;
    }
    /**
     * Get the data for the next monitor.
     * @return the next monitor or null if no new monitorElement are present.
     * If successful releaseEvent must be called before another call to poll.
     */
    MonitorElement poll()
    {
        if(!isStarted) {
            if(!start()) return null;
        }
        monitorElement = monitor.poll();
        return monitorElement;
    }
    /**
     * Wait for a monitor event.
     * The data will be in PvaClientData.
     * @param secondsToWait Time to wait for event.
     * @return (false,true) means event (did not, did) occur.
     */
    boolean waitEvent(double secondsToWait)
    {
        IMPLEMENT
    }
    /**
     * Release the monitorElement returned by poll.
     */
    boolean releaseEvent()
    {
        if(monitorElement==null) {
            String message = channel.getChannelName() + "no monitorElement";
            Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
            setStatus(status);
            return false;
        }
        monitor.release(monitorElement);
        monitorElement = null;
        return true;
    }
    /**
     * Get the data in which monitor events are placed.
     * @return The interface.
     */
    PvaClientMonitorData getData()
    {
        IMPLEMENT
    }  
}
