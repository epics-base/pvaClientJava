/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
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
     * @param channel The channel.
     * @param pvRequest The pvRequest.
     * @return The new instance.
     */
    static PvaClientGet create(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientGet(pvaClient,channel,pvRequest);
    }

    private PvaClientGet(
            PvaClient pvaClient,
            Channel channel, PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.channel = channel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::PvaClientGet"
                + " channelName "  + channel.getChannelName());
        }
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum GetConnectState {connectIdle,connectActive,connected};
    private final PvaClient pvaClient;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForGet = lock.newCondition();

    private PvaClientGetData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status channelGetConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelGetStatus = statusCreate.getStatusOK();
    private volatile ChannelGet channelGet = null;

    private volatile GetConnectState connectState = GetConnectState.connectIdle;

    private enum GetState {getIdle,getActive,getComplete};
    private volatile GetState getState = GetState.getIdle;

    private void checkGetState() {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(connectState==GetConnectState.connectIdle) connect();
        if(getState==GetState.getIdle) get();

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
        lock.lock();
        try {
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientGet::channelGetConnect"
                    + " channelName "  + channel.getChannelName()
                    + " status " + status);
            }
            channelGetConnectStatus = status;
            connectState = GetConnectState.connected;
            this.channelGet = channelGet;
            if(status.isOK()) {
                pvaClientData = PvaClientGetData.create(structure);
                pvaClientData.setMessagePrefix(channel.getChannelName());
            }
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelGetRequester#getDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelGet, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void getDone(Status status, ChannelGet channelGet, PVStructure pvStructure, BitSet bitSet) { 
        if(isDestroyed) return;
        lock.lock();
        try {
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientGet::getDone"
                    + " channelName "  + channel.getChannelName()
                    + " status " + status);
            }
            channelGetStatus = status;
            getState = GetState.getComplete;
            if(status.isOK()) {
                pvaClientData.setData(pvStructure,bitSet);
            }
            waitForGet.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * clean up resources used.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientGet::destroy"
                + " channelName "  + channel.getChannelName());
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
        issueConnect();
        Status status =  waitConnect();
        if(status.isOK()) return;
        String message = "channel "
                + channel.getChannelName() 
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
        if(connectState!=GetConnectState.connectIdle) {
            String message = "channel "
                    + channel.getChannelName() 
                    + " pvaClientGet already connected ";
            throw new RuntimeException(message);
        }
        connectState = GetConnectState.connectActive;
        channelGet = channel.createChannelGet(this, pvRequest);
    }

    /**
     * wait until the channelGet connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        lock.lock();
        try {
            if(connectState==GetConnectState.connected) {
                if(!channelGetConnectStatus.isOK()) connectState = GetConnectState.connectIdle;
                return channelGetConnectStatus;
            }
            if(connectState!=GetConnectState.connectActive) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " pvaClientGet::waitConnect illegal connect state ";
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
        issueGet();
        Status status = waitGet();
        if(status.isOK()) return;
        String message = "channel "
                + channel.getChannelName() 
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
        if(connectState==GetConnectState.connectIdle) connect();
        if(getState==GetState.getActive) {
            String message = "channel "
                    + channel.getChannelName() 
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
        lock.lock();
        try {
            if(getState==GetState.getComplete) {
                return channelGetStatus;
            }
            if(getState!=GetState.getActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientGet::waitGet llegal get state ";
                throw new RuntimeException(message);
            }
            try {
                waitForGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
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
        checkGetState();
        return pvaClientData;
    }

}
