/**
 * 
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.ChannelPut;
import org.epics.pvaccess.client.ChannelPutRequester;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;

/**
 * This is a synchronous alternative to channelPut.
 * @author mrk
 *
 */
public class PvaClientPut implements ChannelPutRequester {

    /**
     * Create a new PvaClientPut
     * @return The interface.
     */
    static PvaClientPut create(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientPut(pvaClient,channel,pvRequest);
    }

    private PvaClientPut(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.channel = channel;
        this.pvRequest = pvRequest;
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();

    private enum PutConnectState {connectIdle,connectActive,connected};
    private final PvaClient pvaClient;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForPutOrGet = lock.newCondition();
    private PvaClientPutData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status channelPutConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelGetPutStatus = statusCreate.getStatusOK();
    private volatile ChannelPut channelPut = null;
    private volatile PutConnectState connectState = PutConnectState.connectIdle;

    private enum PutState {putIdle,getActive,putActive,putComplete};
    private volatile PutState putState = PutState.putIdle;

    void checkPutState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState==PutConnectState.connectIdle)
        {
            connect();
            get();
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
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        pvaClient.message(message, messageType);
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#channelPutConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void channelPutConnect(
            Status status,
            ChannelPut channelPut,
            Structure structure)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");       
        lock.lock();
        try {
            channelPutConnectStatus = status;
            connectState = PutConnectState.connected;
            this.channelPut = channelPut;
            if(status.isOK()) {
                pvaClientData = PvaClientPutData.create(structure);
                pvaClientData.setMessagePrefix(channel.getChannelName());
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
    public void getDone(
            Status status,
            ChannelPut channelPut,
            PVStructure pvStructure,
            BitSet bitSet)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        lock.lock();
        try {
            channelGetPutStatus = status;
            putState = PutState.putComplete;
            if(status.isOK()) {
                PVStructure pvs = pvaClientData.getPVStructure();
                convert.copyStructure(pvStructure,pvs);
                BitSet bs = pvaClientData.getChangedBitSet();
                bs.clear();
                bs.or(bitSet);
            }
            waitForPutOrGet.signal();
        } finally {
            lock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#putDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut)
     */
    @Override
    public void putDone(Status status, ChannelPut channelPut) {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        lock.lock();
        try {
            channelGetPutStatus = status;
            putState = PutState.putComplete;
            waitForPutOrGet.signal();
        } finally {
            lock.unlock();
        }
    }



    /**
     * clean up resources used.
     */
    public void destroy()
    {
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(channelPut!=null) channelPut.destroy();
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
        String message = "channel " + channel.getChannelName()
        + " PvaClientPut::connect " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * create the channelPut connection to the channel.
     * This can only be called once.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState!=PutConnectState.connectIdle) {
            String message = "channel " + channel.getChannelName()
            + "  pvaClientPut already connected";
            throw new RuntimeException(message);
        }
        connectState = PutConnectState.connectActive;
        channelPut = channel.createChannelPut(this, pvRequest);
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
            if(connectState==PutConnectState.connected) {
                if(!channelPutConnectStatus.isOK()) connectState = PutConnectState.connectIdle;
                return channelPutConnectStatus;
            }
            if(connectState!=PutConnectState.connectActive) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " pvaClientGet illegal connect state ";
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
            if(!channelPutConnectStatus.isOK()) connectState = PutConnectState.connectIdle;
            return channelPutConnectStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Call issueGet and then waitGet.
     * An exception is thrown if get fails.
     */
    public void get()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        issueGet();
        Status status = waitGet();
        if(status.isOK()) return;
        String message = "channel " + channel.getChannelName()
        + " PvaClientPut::get " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a get and return immediately.
     */
    public void issueGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState==PutConnectState.connectIdle) connect();
        if(putState!=PutState.putIdle){
            String message = "channel " + channel.getChannelName()
            + " PvaClientPut::issueGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putState = PutState.getActive;
        pvaClientData.getChangedBitSet().clear();
        channelPut.get();
    }
    /**
     * Wait until get completes or for timeout.
     * @return status of get request.
     */
    public Status waitGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        lock.lock();
        try {
            if(putState==PutState.putComplete) {
                putState = PutState.putIdle;
                return channelGetPutStatus;
            }
            if(putState!=PutState.getActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientGet::waitGet llegal get state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutOrGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            putState = PutState.putIdle;
            return channelGetPutStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Call issuePut and then waitPut.
     * @throws RuntimeException if put fails.
     */
    public void put()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        issuePut();
        Status status = waitPut();
        if(status.isOK()) return;
        String message = "channel " + channel.getChannelName()
        + " PvaClientPut::put " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Call channelPut and return.
     */
    public void issuePut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState==PutConnectState.connectIdle) connect();
        if(putState!=PutState.putIdle){
            String message = "channel " + channel.getChannelName()
            + " PvaClientPut::issueGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putState = PutState.putActive;
        channelPut.put(pvaClientData.getPVStructure(),pvaClientData.getChangedBitSet());
    }

    /**
     * Wait until put completes.
     * @return status of put request
     */
    public Status waitPut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        lock.lock();
        try {
            if(putState==PutState.putComplete) {
                putState = PutState.putIdle;
                return channelGetPutStatus;
            }
            if(putState!=PutState.putActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientGet::waitGet llegal put state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutOrGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            putState = PutState.putIdle;
            if(channelGetPutStatus.isOK()) pvaClientData.getChangedBitSet().clear();
            return channelGetPutStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the data
     * @return The interface.
     */
    public PvaClientPutData getData()
    {
        checkPutState();
        return pvaClientData;
    }  
}
