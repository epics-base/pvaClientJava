/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.ChannelProcess;
import org.epics.pvaccess.client.ChannelProcessRequester;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;

/**
 * An easy interface to channelProcess.
 * @author mrk
 * @since 2015.06
 */
public class PvaClientProcess implements ChannelProcessRequester{
    /**
     * Create new PvaClientProcess.
     * @param pvaClient The single instance of pvaClient.
     * @param channel The channel.
     * @param pvRequest The pvRequest.
     * @return The new instance.
     */
    static PvaClientProcess create(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientProcess(pvaClient,channel,pvRequest);
    }
    private PvaClientProcess(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.channel = channel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientProcess::PvaClientProcess()");
        }
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum ProcessConnectState {connectIdle,connectActive,connected};
    private final PvaClient pvaClient;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForProcess = lock.newCondition();


    private volatile boolean isDestroyed = false;
    private volatile Status channelProcessConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelProcessStatus = statusCreate.getStatusOK();
    private volatile ChannelProcess channelProcess = null;

    private volatile ProcessConnectState connectState = ProcessConnectState.connectIdle;

    private enum ProcessState {processIdle,processActive,processComplete};
    private volatile ProcessState processState = ProcessState.processIdle;

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#getRequesterName()
     */
    @Override
    public String getRequesterName() {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        return pvaClient.getRequesterName();
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#message(java.lang.String, org.epics.pvdata.pv.MessageType)
     */
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        pvaClient.message(message, messageType);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelProcessRequester#channelProcessConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelProcess)
     */
    @Override
    public void channelProcessConnect(Status status, ChannelProcess channelProcess) {
        if(isDestroyed) return;
        lock.lock();
        try {
            channelProcessConnectStatus = status;
            connectState = ProcessConnectState.connected;
            this.channelProcess = channelProcess;
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelProcessRequester#processDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelProcess)
     */
    @Override
    public void processDone(Status status, ChannelProcess channelProcess) { 
        if(isDestroyed) return;
        lock.lock();
        try {
            channelProcessStatus = status;
            processState = ProcessState.processComplete;
            waitForProcess.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Destroy the EasyProcess.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientProcess::destroy");
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(channelProcess!=null) channelProcess.destroy();
    }
    /**
     * Call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        issueConnect();
        Status status =  waitConnect();
        if(status.isOK()) return;
        String message = "channel "
                + channel.getChannelName() 
                + " PvaClientProcess::connect "
                +  status.getMessage();
        throw new RuntimeException(message);
    }
    /**
     * Issue a connect request and return immediately.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        if(connectState!=ProcessConnectState.connectIdle) {
            String message = "channel "
                    + channel.getChannelName() 
                    + " pvaClientProcess already connected ";
            throw new RuntimeException(message);
        }
        connectState = ProcessConnectState.connectActive;
        channelProcess = channel.createChannelProcess(this, pvRequest);
    }
    /**
     * Wait until connection completes.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        lock.lock();
        try {
            if(connectState==ProcessConnectState.connected) {
                if(!channelProcessConnectStatus.isOK()) connectState = ProcessConnectState.connectIdle;
                return channelProcessConnectStatus;
            }
            if(connectState!=ProcessConnectState.connectActive) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " pvaClientProcess illegal connect state ";
                throw new RuntimeException(message);
            }
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            if(!channelProcessConnectStatus.isOK()) connectState = ProcessConnectState.connectIdle;
            return channelProcessConnectStatus;
        } finally {
            lock.unlock();
        }

    }
    /**
     * Call issueProcess and then waitProcess.
     * An exception is thrown if process fails
     */
    public void process()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        issueProcess();
        Status status = waitProcess();
        if(status.isOK()) return;
        String message = "channel "
                + channel.getChannelName() 
                +  "PvaClientProcess::process "
                + status.getMessage();
        throw new RuntimeException(message);
    }
    /**
     * Issue a process request and return immediately.
     */
    public void issueProcess()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        if(connectState==ProcessConnectState.connectIdle) connect();
        if(processState!=ProcessState.processIdle) {
            String message = "channel "
                    + channel.getChannelName() 
                    +  " PvaClientProcess::issueProcess process aleady active ";
            throw new RuntimeException(message);
        }
        processState = ProcessState.processActive;
        channelProcess.process();
    }
    /**
     * Wait until process completes or for timeout.
     * @return status of process request.
     */
    public Status waitProcess()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientProcess was destroyed");
        lock.lock();
        try {
            if(processState==ProcessState.processComplete) {
                processState = ProcessState.processIdle;
                return channelProcessStatus;
            }
            if(processState!=ProcessState.processActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientProcess::waitProcess llegal process state ";
                throw new RuntimeException(message);
            }
            try {
                waitForProcess.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            processState = ProcessState.processIdle;
            return channelProcessStatus;
        } finally {
            lock.unlock();
        }
    }
}
