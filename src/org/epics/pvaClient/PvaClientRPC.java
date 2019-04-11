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
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;

/**
 * This is a synchronous alternative to channelRPC.
 * @author mrk
 * @since 2016.06
 */
public class PvaClientRPC implements ChannelRPCRequester{
    /**
     * Create an instance of PvaClientRPC.
     * @param pvaClient The single instance of pvaClient.
     * @param channel The channel.
     * @return The new instance.
     */
    static PvaClientRPC create(
            PvaClient pvaClient,
            Channel channel)
    {
        return create(pvaClient,channel,null);
    }
    /**
     * Create an instance of PvaClientRPC.
     * @param pvaClient The single instance of pvaClient.
     * @param channel The channel.
     * @param pvRequest The pvRequest.
     * @return The new instance.
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
        if(PvaClient.getDebug()) System.out.println("PvaClientRPC::PvaClientRPC");
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
    private Status status = null;
    private PVStructure pvResponse = null;
    
    private enum RPCState {rpcIdle,rpcActive,rpcComplete};
    private volatile RPCState rpcState = RPCState.rpcIdle;
    private double responseTimeout = 0.0;

    
    void checkRPCState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");
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
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");
        pvaClient.message(message, messageType);
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRPCRequester#channelRPCConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelRPC, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void channelRPCConnect(
            Status status,
            ChannelRPC channelRPC)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");       
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
     * @see org.epics.pvaccess.client.ChannelRPCRequester#getDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelRPC, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void requestDone(
            Status status,
            ChannelRPC channelRPC,
            PVStructure pvResponse)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");
        lock.lock();
        try {
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientRPC::requestDone() "
                        + "channel " + channel.getChannelName()
                        + " status.isOK " + status.isOK());
            }
            if(rpcState!=RPCState.rpcActive) {
                String message = "channel " 
                        + channel.getChannelName()
                        + " PvaClientRPC::requestDone but not active "; 
                   throw new RuntimeException(message);
            }
            if(pvaClientRPCRequester!=null && responseTimeout<=0.0) {
                rpcState = RPCState.rpcIdle;
            } else {
                rpcState = RPCState.rpcComplete;
                if(pvaClientRPCRequester==null) {
                    this.status = status;
                    this.pvResponse = pvResponse;
                }
                waitForDone.signal();
            }
        } finally {
            lock.unlock();
        }
        if(pvaClientRPCRequester!=null) {
            pvaClientRPCRequester.requestDone(status, this, pvResponse);
        }
    }

    /**
     * clean up resources used.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientRPC::destroy");
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(channelRPC!=null) channelRPC.destroy();
    }
    /**
     * Set a timeout for a request.
     * @param responseTimeout The time in seconds to wait for a request to complete.
     */
    public void setResponseTimeout(double responseTimeout) 
    {
        this.responseTimeout = responseTimeout;
    }
    /**
     * Get the responseTimeout.
     * @return The value.
     */
    public double getResponseTimeout()
    {
        return responseTimeout;
    }
    /**
     * call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");
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
     * create the channelRPC connection to the channel.
     * This can only be called once.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");
        if(connectState!=RPCConnectState.connectIdle) {
            String message = "channel " + channel.getChannelName()
            + "  pvaClientRPC already connected";
            throw new RuntimeException(message);
        }
        connectState = RPCConnectState.connectActive;
        channelRPC = channel.createChannelRPC(this, pvRequest);
    }

    /**
     * wait until the channelRPC connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientRPC was destroyed");
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

    /**
     * Issue a request.
     * @param pvArgument The argument for the request.
     * @return The result.
     */
    public PVStructure request(PVStructure pvArgument)
            throws RPCRequestException
    {
        checkRPCState();
        if(rpcState!=RPCState.rpcIdle) {
            String message = "channel "
                    + channel.getChannelName() 
                    + " PvaClientRPC::request request aleady active ";
            throw new RuntimeException(message);
        }
        rpcState = RPCState.rpcActive;
        channelRPC.request(pvArgument);
        lock.lock();
        try {
            if(rpcState!=RPCState.rpcComplete) {
                try {
                    if(responseTimeout>0.0) {
                        long nano = (long)(responseTimeout*1e9);
                        long ret = waitForDone.awaitNanos(nano);
                        if(ret<=0) {
                            String message = "channel "
                                    + channel.getChannelName() + " request timeout";
                            throw new RuntimeException(message);
                        }
                    } else {
                        waitForDone.await();
                    }
                } catch(InterruptedException e) {
                    String message = "channel "
                            + channel.getChannelName() 
                            + " InterruptedException " + e.getMessage();
                    if(pvaClientRPCRequester!=null) {
                        Status status = statusCreate.createStatus(StatusType.ERROR, message,null);
                        pvaClientRPCRequester.requestDone(status,this,null);
                    } else {
                        throw new RuntimeException(message);
                    }
                }
            }
            rpcState = RPCState.rpcIdle;
            if (status.isSuccess()) {
                return pvResponse;
            } else {
                if (status.getStackDump() == null)
                    throw new RPCRequestException(status.getType(), status.getMessage());
                else
                    throw new RPCRequestException(status.getType(), status.getMessage() + ", cause:\n" + status.getStackDump());
            }
        } finally {
            lock.unlock();
        }  
    }
    

    /**
     * Issue a request.
     * Note that if responseTimeout is ( lt 0.0, ge 0.0) then this (will, will not) block
     * until response completes or timeout.
     * @param pvArgument The argument for the request.
     * @param pvaClientRPCRequester The client requester to call when the request completes.
     */
    public void request(
        PVStructure pvArgument,
        PvaClientRPCRequester pvaClientRPCRequester)
                throws RPCRequestException
    {
        this.pvaClientRPCRequester = pvaClientRPCRequester;
        checkRPCState();
        if(responseTimeout<=0.0) {
            lock.lock();
            try {
            if(rpcState!=RPCState.rpcIdle) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " PvaClientRPC::request request aleady active ";
                throw new RuntimeException(message);
            }
            rpcState = RPCState.rpcActive;
            } finally {
                lock.unlock();
            }  
            channelRPC.request(pvArgument);
            return;
        }
        request(pvArgument);
    }
}
