/**
 * 
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
 * This is an easy to use alternative to ChannelGet
 * @author mrk
 *
 */
public class PvaClientGet implements ChannelGetRequester
{
    /**
     * Create new PvaClientGet.
     * @return The interface.
     */
    static PvaClientGet create(
            PvaClient pvaClient,
            PvaClientChannel easyChannel,
            Channel channel, PVStructure pvRequest)
    {
        return new PvaClientGet(pvaClient,easyChannel,channel,pvRequest);
    }

    public PvaClientGet(
            PvaClient pvaClient,
            PvaClientChannel easyChannel,
            Channel channel, PVStructure pvRequest)
    {

        this.pvaClientChannel = easyChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();

    private enum GetConnectState {connectIdle,connectActive,connected};
    private final PvaClientChannel pvaClientChannel;
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

    private enum GetState {getIdle,getActive,getDone};
    private volatile GetState getState = GetState.getIdle;

    @Override
    public String getRequesterName() {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        return pvaClientChannel.getRequesterName();
    }

    @Override
    public void message(String message, MessageType messageType) {
        pvaClientChannel.message(message, messageType);
    }

    @Override
    public void channelGetConnect(Status status, ChannelGet channelGet, Structure structure) {
        if(isDestroyed) return;
        channelGetConnectStatus = status;
        this.channelGet = channelGet;
        if(status.isOK()) {
            pvaClientData = PvaClientGetData.create(structure);
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
    public void getDone(Status status, ChannelGet channelGet, PVStructure pvStructure, BitSet bitSet) { 
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        channelGetStatus = status;
        if(status.isOK()) {
            pvaClientData.setData(pvStructure,bitSet);
        }

        lock.lock();
        try {
            waitForGet.signal();
        } finally {
            lock.unlock();
        }
    }

    private void checkGetState() {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(connectState==GetConnectState.connectIdle) connect();
        if(getState==GetState.getIdle) get();

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
        if(channelGet!=null) channelGet.destroy();
        if(pvaClientData!=null) pvaClientData = null;
    }

    /**
     * call issueConnect and then waitConnect.
     * An exception is thrown if connect fails.
     */
    void connect()
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
    void issueConnect()
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
     * @return status
     */
    Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(connectState!=GetConnectState.connectActive) {
            String message = "channel "
                    + channel.getChannelName() 
                    + " pvaClientGet illegal connect state ";
            throw new RuntimeException(message);
        }
        try {
            lock.lock();
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
        } finally {
            lock.unlock();
        }
        if(channelGetConnectStatus.isOK()){
            connectState = GetConnectState.connected;
            return statusCreate.getStatusOK();
        }
        connectState = GetConnectState.connectIdle;
        return channelGetConnectStatus;
    }

    /**
     * Call issueGet and then waitGet.
     * An exception is thrown if get fails
     */
    void get()
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
    void issueGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(connectState==GetConnectState.connectIdle) connect();
        if(getState!=GetState.getIdle) {
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
     * @return status
     */
    Status waitGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientGet was destroyed");
        if(getState!=GetState.getActive){
            String message = "channel "
                    + channel.getChannelName() 
                    +  " PvaClientGet::waitGet llegal get state ";
            throw new RuntimeException(message);
        }
        try {
            lock.lock();
            try {
                if(getState==GetState.getActive) waitForGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
        } finally {
            lock.unlock();
        }

        getState = GetState.getIdle;
        if(channelGetStatus.isOK()) {
            return statusCreate.getStatusOK();
        }
        return channelGetStatus;
    }

    /**
     * Get the data/
     * @return The interface.
     */
    PvaClientGetData getData()
    {
        checkGetState();
        return pvaClientData;
    }

}
