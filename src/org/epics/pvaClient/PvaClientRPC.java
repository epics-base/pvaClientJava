/**
 * 
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
 * An easy to use interface to channelRPC.
 * @author mrk
 *
 */
public class PvaClientRPC implements ChannelRPCRequester{

    /**
     * Create a new PvaClientRPC.
     * @return The interface.
     */
    static PvaClientRPC create(PvaClientChannel easyChannel, Channel channel, PVStructure pvRequest) {
        return new PvaClientRPC(easyChannel,channel,pvRequest);
    }

    PvaClientRPC(PvaClientChannel easyChannel,Channel channel,PVStructure pvRequest) {
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
    private final Condition waitForRPC = lock.newCondition();

    private volatile boolean isDestroyed = false;

    private volatile Status status = statusCreate.getStatusOK();
    private volatile ChannelRPC channelRPC = null;
    private volatile PVStructure result = null;

    private enum ConnectState {connectIdle,notConnected,connected};
    private volatile ConnectState connectState = ConnectState.connectIdle;

    private boolean checkConnected() {
        if(connectState==ConnectState.connectIdle) connect();
        if(connectState==ConnectState.connected) return true;
        if(connectState==ConnectState.notConnected) {
            String message = channel.getChannelName() + " rpc not connected";
            Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
            setStatus(status);
            return false;
        }
        String message = channel.getChannelName() + " illegal rpcConnect state";
        Status status = statusCreate.createStatus(StatusType.ERROR, message, null);
        setStatus(status);
        return false;

    }

    private enum RPCState {rpcIdle,rpcActive,rpcDone};
    private RPCState rpcState = RPCState.rpcIdle;
    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#getRequesterName()
     */
    @Override
    public String getRequesterName() {
        return easyChannel.getRequesterName();
    }
    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#message(java.lang.String, org.epics.pvdata.pv.MessageType)
     */
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) return;
        easyChannel.message(message, messageType);
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRPCRequester#channelRPCConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelRPC)
     */
    @Override
    public void channelRPCConnect(Status status, ChannelRPC channelRPC) {
        if(isDestroyed) return;
        this.channelRPC = channelRPC;
        this.status = status;
        if(!status.isSuccess()) {
            connectState = ConnectState.notConnected;
        } else {
            connectState = ConnectState.connected;
        }
        lock.lock();
        try {
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRPCRequester#requestDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelRPC, org.epics.pvdata.pv.PVStructure)
     */
    @Override
    public void requestDone(Status status, ChannelRPC channelRPC, PVStructure result) {
        this.status = status;
        this.result = result;
        rpcState = RPCState.rpcDone;
        lock.lock();
        try {
            waitForRPC.signal();
        } finally {
            lock.unlock();
        }
    }




    /**
     * Disconnect from channel and destroy all resources.
     */
    void destroy()
    {
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        channelRPC.destroy();
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
        channelRPC = channel.createChannelRPC(this, pvRequest);
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
        if(connectState==ConnectState.connectIdle) {
            Status status = statusCreate.createStatus(StatusType.ERROR," did not connect",null);
            setStatus(status);
            return false;
        }
        return true;
    }
    /**
     * Call issueRequest and the waitRequest.
     * @param request The request pvStructure.
     * @return The result pvStructure.
     */
    PVStructure request(PVStructure request)
    {
        issueRequest(request);
        return waitRequest();
    }
    /**
     * call Monitor::request and return.
     * @param request The request pvStructure.
     */
    void issueRequest(PVStructure request)
    {
        if(isDestroyed) return;
        if(!checkConnected()) return;
        if(rpcState!=RPCState.rpcIdle) {
            Status status = statusCreate.createStatus(
                    StatusType.ERROR,"rpc already issued",null);
            setStatus(status);
            return;
        }
        rpcState = RPCState.rpcActive;
        channelRPC.request(request);
    }
    /**
     * Wait until Monitor::request completes or for timeout.
     * If failure null is returned.
     * If failure getStatus can be called to get reason.
     * @return The result pvStructure.
     */
    PVStructure waitRequest()
    {
        if(isDestroyed) return null;
        if(!checkConnected()) return null;
        try {
            lock.lock();
            try {
                if(rpcState==RPCState.rpcActive) waitForRPC.await();
            } catch(InterruptedException e) {
                Status status = statusCreate.createStatus(StatusType.ERROR, e.getMessage(), e.fillInStackTrace());
                setStatus(status);
                return null;
            }
        } finally {
            lock.unlock();
        }
        if(rpcState==RPCState.rpcActive) {
            Status status = statusCreate.createStatus(StatusType.ERROR," rpc failed",null);
            setStatus(status);
            return null;
        }
        rpcState = RPCState.rpcIdle;
        return result;
    }
}
