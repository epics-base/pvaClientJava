/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.ChannelPut;
import org.epics.pvaccess.client.ChannelPutRequester;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;

/**
 * This is a synchronous alternative to channelPut.
 * @author mrk
 * @since 2015.06
 */
public class PvaClientPut implements ChannelPutRequester {

    /**
     * Create a new PvaClientPut
     * @return The interface.
     */
    static PvaClientPut create(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        return new PvaClientPut(pvaClient,pvaClientChannel,pvRequest);
    }

    private PvaClientPut(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::PvaClientPut()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        this.pvaClient = pvaClient;
        this.pvaClientChannel = pvaClientChannel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) System.out.println("PvaClientPut::PvaClientPut");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();

    private enum PutConnectState {connectIdle,connectActive,connected};
    private enum PutState {putIdle,getActive,putActive,putComplete};
    private final PvaClient pvaClient;
    private final PvaClientChannel pvaClientChannel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForGetPut = lock.newCondition();
    private PvaClientPutData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status channelPutConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelGetPutStatus = statusCreate.getStatusOK();
    private volatile ChannelPut channelPut = null;
    private volatile PutConnectState connectState = PutConnectState.connectIdle;

    
    private volatile PutState putState = PutState.putIdle;
    private volatile PvaClientPutRequester pvaClientPutRequester = null;
   
    void checkPutState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::checkPutState()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==PutConnectState.connectIdle)
        {
            connect();
        }
        if(connectState==PutConnectState.connectActive)
        {
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
                    + " "
                    + channelPutConnectStatus.getMessage();
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
        if(isDestroyed) return;
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
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::channelPutConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName()
                 + " status.isOK " + status.isOK());
        }
        lock.lock();
        try {
            if(status.isOK()) {
                channelPutConnectStatus = status;
                connectState = PutConnectState.connected;
                pvaClientData = PvaClientPutData.create(structure);
                pvaClientData.setMessagePrefix(channelPut.getChannel().getChannelName());
            } else {
                 String message = "PvaClientPut::channelPutConnect"
                   + "\npvRequest\n" + pvRequest
                   + "\nerror\n" + status.getMessage();
                 channelPutConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,message,null);
            }
            this.channelPut = channelPut;
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutRequester!=null) pvaClientPutRequester.channelPutConnect(status,this);
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
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::getDone"
                + " channelName " + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        }
        lock.lock();
        try {   
            channelGetPutStatus = status;
            if(status.isOK()) {
                PVStructure pvs = pvaClientData.getPVStructure();
                convert.copyStructure(pvStructure,pvs);
                BitSet bs = pvaClientData.getChangedBitSet();
                bs.clear();
                bs.or(bitSet);
            }
            putState = PutState.putComplete;
            waitForGetPut.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutRequester!=null) pvaClientPutRequester.getDone(status,this);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutRequester#putDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPut)
     */
    @Override
    public void putDone(Status status, ChannelPut channelPut) {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::putDone"
                + " channelName " + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        }
        lock.lock();
        try {
            channelGetPutStatus = status;
            putState = PutState.putComplete;
            waitForGetPut.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutRequester!=null) pvaClientPutRequester.putDone(status,this);
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
        if(channelPut!=null) channelPut.destroy();
    }

    /**
     * call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::connect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueConnect();
        Status status = waitConnect();
        if(status.isOK()) return;
        String message = "channel " 
             + pvaClientChannel.getChannel().getChannelName()
             + " PvaClientPut::connect " 
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
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::issueConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState!=PutConnectState.connectIdle) {
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
            + "  pvaClientPut already connected";
            throw new RuntimeException(message);
        }
        connectState = PutConnectState.connectActive;
        channelPutConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,"connect active",null);
        channelPut = pvaClientChannel.getChannel().createChannelPut(this, pvRequest);
    }

    /**
     * wait until the channelPut connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::waitConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(connectState==PutConnectState.connected) {
                if(!channelPutConnectStatus.isOK()) connectState = PutConnectState.connectIdle;
                return channelPutConnectStatus;
            }
            if(connectState!=PutConnectState.connectActive) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " pvaClientGet illegal connect state ";
                return statusCreate.createStatus(StatusType.ERROR, message,null);
            }
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                return statusCreate.createStatus(StatusType.ERROR, message,e.fillInStackTrace());
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
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::get()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueGet();
        Status status = waitGet();
        if(status.isOK()) return;
        String message = "channel " 
            + pvaClientChannel.getChannel().getChannelName()
            + " PvaClientPut::get " 
            + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a get and return immediately.
     */
    public void issueGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::issueGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==PutConnectState.connectIdle) connect();
        if(putState==PutState.getActive || putState==PutState.putActive){
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
            + " PvaClientPut::issueGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putState = PutState.getActive;
        channelPut.get();
    }
    /**
     * Wait until get completes or for timeout.
     * @return status of get request.
     */
    public Status waitGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::waitGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(putState==PutState.putComplete) return channelGetPutStatus;
            if(putState!=PutState.getActive){
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        +  " PvaClientPut::waitGet llegal putGet state ";
                throw new RuntimeException(message);
            }
            try {
                waitForGetPut.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
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
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::put()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issuePut();
        Status status = waitPut();
        if(status.isOK()) return;
        String message = "channel " 
            + pvaClientChannel.getChannel().getChannelName()
            + " PvaClientPut::put " 
            + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Call channelPut and return.
     */
    public void issuePut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::issuePut()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==PutConnectState.connectIdle) connect();
        if(putState==PutState.getActive || putState==PutState.putActive){
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
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
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::waitPut()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(putState==PutState.putComplete) return channelGetPutStatus;
            if(putState!=PutState.putActive){
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        +  " PvaClientPut::waitPut llegal putGet state ";
                throw new RuntimeException(message);
            }
            try {
                waitForGetPut.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
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
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::getData()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        checkPutState();
        if(putState==PutState.putIdle) get();
        return pvaClientData;
    }
    /**
     * Set a user callback.
     * @param pvaClientPutRequester The requester which must be implemented by the caller.
     */
    public void setRequester(PvaClientPutRequester pvaClientPutRequester)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPut::setRequester()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        this.pvaClientPutRequester = pvaClientPutRequester;
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
