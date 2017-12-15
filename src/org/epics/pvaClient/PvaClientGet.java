/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.ChannelGet;
import org.epics.pvaccess.client.ChannelGetRequester;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;

/**
 * This is a synchronous alternative to ChannelGet
 * @author mrk
 * @since 2015.06
 */
public class PvaClientGet implements ChannelGetRequester
{
    /**
     * Create an instance of PvaClientGet.
     * @param pvaClient The single instance of pvaClient.
     * @param pvaClientChannel The pvaClientChannel.
     * @param pvRequest The pvRequest.
     * @return The new instance.
     */
    static PvaClientGet create(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        return new PvaClientGet(pvaClient,pvaClientChannel,pvRequest);
    }

    private PvaClientGet(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::PvaClientGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName()
                 + " pvRequest " + pvRequest);
        }
        this.pvaClient = pvaClient;
        this.pvaClientChannel = pvaClientChannel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::PvaClientGet"
                + " channelName "  + pvaClientChannel.getChannel().getChannelName());
        }
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum GetConnectState {connectIdle,connectActive,connected};
    private enum GetState {getIdle,getActive,getComplete};
    private final PvaClient pvaClient;
    private final PvaClientChannel pvaClientChannel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForGet = lock.newCondition();

    private PvaClientGetData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status channelGetConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelGetStatus = statusCreate.getStatusOK();
    private volatile GetConnectState connectState = GetConnectState.connectIdle;
    private volatile ChannelGet channelGet = null;
    private volatile PvaClientGetRequester pvaClientGetRequester = null;
    private volatile GetState getState = GetState.getIdle;
    

    private void checkGetState() {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::checkGetState()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==GetConnectState.connectIdle) connect();
        if(connectState==GetConnectState.connectActive)
        {
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
                    + " "
                    + channelGetConnectStatus.getMessage();
            throw new RuntimeException(message);
        }
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
        pvaClient.message(message, messageType);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelGetRequester#channelGetConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelGet, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void channelGetConnect(Status status, ChannelGet channelGet, Structure structure) {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::channelGetConnect"
                + " channelName "  + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        }
        lock.lock();
        try {
            if(status.isOK()) {
                channelGetConnectStatus = status;
                connectState = GetConnectState.connected;
                pvaClientData = PvaClientGetData.create(structure);
                pvaClientData.setMessagePrefix(channelGet.getChannel().getChannelName());
            } else {
                 String message = "PvaClientGet::channelGetConnect"
                   + "\npvRequest\n" + pvRequest
                   + "\nerror\n" + status.getMessage();
                 channelGetConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,message,null);
            }
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientGetRequester!=null) pvaClientGetRequester.channelGetConnect(status,this);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelGetRequester#getDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelGet, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void getDone(Status status, ChannelGet channelGet, PVStructure pvStructure, BitSet bitSet) { 
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::getDone"
                + " channelName "  + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        }
        lock.lock();
        try {   
            channelGetStatus = status;
            getState = GetState.getComplete;
            if(status.isOK()) {
                pvaClientData.setData(pvStructure,bitSet);
            }
            waitForGet.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientGetRequester!=null) pvaClientGetRequester.getDone(status,this);
    }

    /**
     * clean up resources used.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::destroy"
                + " channelName "  + pvaClientChannel.getChannel().getChannelName());
        }
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(channelGet!=null) channelGet.destroy();
        if(pvaClientData!=null) pvaClientData = null;
    }

    /**
     * call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::connect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueConnect();
        Status status =  waitConnect();
        if(status.isOK()) return;
        String message = "channel "
                + pvaClientChannel.getChannel().getChannelName() 
                + " PvaClientGet::connect "
                +  status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * create the channelGet connection to the channel.
     * This can only be called once.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::issueConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState!=GetConnectState.connectIdle) {
            String message = "channel "
                    + pvaClientChannel.getChannel().getChannelName() 
                    + " pvaClientGet already connected ";
            throw new RuntimeException(message);
        }
        connectState = GetConnectState.connectActive;
        channelGetConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,"connect active",null);
        channelGet = pvaClientChannel.getChannel().createChannelGet(this, pvRequest);
    }

    /**
     * wait until the channelGet connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::waitConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(connectState==GetConnectState.connected) {
                if(!channelGetConnectStatus.isOK()) connectState = GetConnectState.connectIdle;
                return channelGetConnectStatus;
            }
            if(connectState!=GetConnectState.connectActive) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " pvaClientGet::waitConnect illegal connect state ";
                throw new RuntimeException(message);
            }
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            if(!channelGetConnectStatus.isOK()) connectState = GetConnectState.connectIdle;
            return channelGetConnectStatus;
        } finally {
            lock.unlock();
        }

    }
    /**
     * Call issueGet and then waitGet.
     * @throws RuntimeException if create fails.
     */
    public void get()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::get()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueGet();
        Status status = waitGet();
        if(status.isOK()) return;
        String message = "channel "
                + pvaClientChannel.getChannel().getChannelName() 
                +  "PvaClientGet::get "
                + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a get and return immediately.
     */
    public void issueGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::issueGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==GetConnectState.connectIdle) connect();
        if(getState==GetState.getActive) {
            String message = "channel "
                    + pvaClientChannel.getChannel().getChannelName() 
                    +  " PvaClientGet::issueGet get aleady active ";
            throw new RuntimeException(message);
        }
        getState = GetState.getActive;
        channelGet.get();
    }

    /**
     * Wait until get completes.
     * @return status of get request.
     */
    public Status waitGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::waitGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(getState==GetState.getComplete) {
                getState=GetState.getIdle;
                return channelGetStatus;
            }
            if(getState!=GetState.getActive){
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        +  " PvaClientGet::waitGet llegal get state ";
                throw new RuntimeException(message);
            }
            try {
                waitForGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            getState = GetState.getComplete;
            return channelGetStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the data for the channelGet.
     * @return The interface.
     */
    public PvaClientGetData getData()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::getData()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        checkGetState();
        if(getState==GetState.getIdle) get();
        return pvaClientData;
    }
    /**
     * Set a user callback.
     * @param pvaClientGetRequester The requester which must be implemented by the caller.
     */
    public void setRequester(PvaClientGetRequester pvaClientGetRequester)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::setRequester()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        this.pvaClientGetRequester = pvaClientGetRequester;
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
