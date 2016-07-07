/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
/**
 * @author mrk
 * @date 2016.07
 */

package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.ChannelRPC;
import org.epics.pvaccess.client.ChannelRPCRequester;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;

/**
 * This is a synchronous alternative to channelRPC.
 *
 */
public class PvaClientRPC implements ChannelRPCRequester{

    /**
     * Create a new PvaClientPut
     * @return The interface.
     */
    static PvaClientRPC create(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientRPC(pvaClient,channel,pvRequest);
    }

    private PvaClientRPC(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.channel = channel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) System.out.println("PvaClientPut::PvaClientPut");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum RPCConnectState {connectIdle,connectActive,connected};
    private volatile RPCConnectState connectState = RPCConnectState.connectIdle;
    private volatile Status connectStatus = statusCreate.getStatusOK();
    
    private final PvaClient pvaClient;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForDone = lock.newCondition();
    private PvaClientRPCRequester pvaClientRPCRequester = null;

    private volatile boolean isDestroyed = false;
    private volatile ChannelRPC channelRPC = null;
    private PVStructure pvResponse = null;

    
    void checkRPCState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientChannel::waitConnect() "
                    + "channel " + channel.getChannelName());
        }
        if(connectState==RPCConnectState.connectIdle) connect();
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#getRequesterName()
     */
    @Override
    public String getRequesterName() {
        return pvaClient.getRequesterName();
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#message(java.lang.String, org.epics.pvdata.pv.MessageType)
     */
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        pvaClient.message(message, messageType);
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#channelPutConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void channelRPCConnect(
            Status status,
            ChannelRPC channelRPC)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");       
        lock.lock();
        try {
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientRPC::channelRPCConnect() "
                        + "channel " + channel.getChannelName()
                        + " status.isOK " + status.isOK());
            }
            connectStatus = status;
            connectState = RPCConnectState.connected;
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientRPC::channelRPCConnect() calling waitForConnect.signal");
            }
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#getDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void requestDone(
            Status status,
            ChannelRPC channelRPC,
            PVStructure pvResponse)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        lock.lock();
        try {
            if(pvaClientRPCRequester!=null) {
                pvaClientRPCRequester.requestDone(status, this, pvResponse);
                return;
            }
            this.pvResponse = pvResponse;
            waitForDone.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * clean up resources used.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientPut::destroy");
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(channelRPC!=null) channelRPC.destroy();
    }

    /**
     * call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        issueConnect();
        Status status = waitConnect();
        if(status.isOK()) return;
        String message = "channel " 
             + channel.getChannelName()
             + " PvaClientRPC::connect " 
             + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * create the channelPut connection to the channel.
     * This can only be called once.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState!=RPCConnectState.connectIdle) {
            String message = "channel " + channel.getChannelName()
            + "  pvaClientRPC already connected";
            throw new RuntimeException(message);
        }
        connectState = RPCConnectState.connectActive;
        channelRPC = channel.createChannelRPC(this, pvRequest);
    }

    /**
     * wait until the channelPyt connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        lock.lock();
        try {
            if(connectState==RPCConnectState.connected) {
                if(!connectStatus.isOK()) connectState = RPCConnectState.connectIdle;
                return connectStatus;
            }
            if(connectState!=RPCConnectState.connectActive) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " pvaClientRPC illegal connect state ";
                return statusCreate.createStatus(StatusType.ERROR, message,null);
            }
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                return statusCreate.createStatus(StatusType.ERROR, message,e.fillInStackTrace());
            }
            if(!connectStatus.isOK()) connectState = RPCConnectState.connectIdle;
            return connectStatus;
        } finally {
            lock.unlock();
        }
    }

    public PVStructure request(PVStructure pvArgument)
    {
        checkRPCState();
        channelRPC.request(pvArgument);
        lock.lock();
        try {
            try {
                waitForDone.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            return pvResponse;
        } finally {
            lock.unlock();
        }  
    }
    

    public void request(
        PVStructure pvArgument,
        PvaClientRPCRequester pvaClientRPCRequester)
    {
        this.pvaClientRPCRequester = pvaClientRPCRequester;
        channelRPC.request(pvArgument);
       
    }
}
