/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.ChannelPutGet;
import org.epics.pvaccess.client.ChannelPutGetRequester;
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
 *
 * This is a synchronous alternative to channelPutGet.
 * @author mrk
 * @since 2015.06
 */
public class PvaClientPutGet implements ChannelPutGetRequester
{

    /**
     * Create new PvaClientPutGet.
     * @param pvaClient The pvaClient
     * @param pvaClientChannel The pvaClientChannel.
     * @param pvRequest The pvRequest.
     * @return The interface.
     */
    static public PvaClientPutGet create(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        return new PvaClientPutGet(pvaClient,pvaClientChannel,pvRequest);
    }

    private PvaClientPutGet(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.pvaClientChannel = pvaClientChannel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) System.out.println("PvaClientPutGet::PvaClientPutGet");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();

    private enum PutGetConnectState {connectIdle,connectActive,connected};
    private enum PutGetState {putGetIdle,putGetActive,putGetComplete};
    private final PvaClient pvaClient;
    private final PvaClientChannel pvaClientChannel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForPutGet = lock.newCondition();
    private PvaClientGetData pvaClientGetData = null;
    private PvaClientPutData pvaClientPutGetData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status channelPutGetConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelPutGetStatus = statusCreate.getStatusOK();
    private volatile ChannelPutGet channelPutGet = null;
    private volatile PutGetConnectState connectState = PutGetConnectState.connectIdle;
    
    private volatile PutGetState putGetState = PutGetState.putGetIdle;
    private volatile PvaClientPutGetRequester pvaClientPutGetRequester = null;

    void checkPutGetState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGetGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::checkPutGetState()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==PutGetConnectState.connectIdle)
        {
            connect();
        }
        if(connectState==PutGetConnectState.connectActive)
        {
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
                    + " "
                    + channelPutGetConnectStatus.getMessage();
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
     * @see org.epics.pvaccess.client.ChannelPutGetRequester#channelPutGetConnect(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPutGet, org.epics.pvdata.pv.Structure, org.epics.pvdata.pv.Structure)
     */
    @Override
    public void channelPutGetConnect(
            Status status,
            ChannelPutGet channelPutGet,
            Structure putStructure,
            Structure getStructure)
    {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::checkPutGetConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            this.channelPutGet = channelPutGet;
            if(status.isOK()) {
                channelPutGetConnectStatus = status;
                connectState = PutGetConnectState.connected;
                pvaClientPutGetData = PvaClientPutData.create(putStructure);
                pvaClientPutGetData.setMessagePrefix(pvaClientChannel.getChannel().getChannelName());
                pvaClientGetData = PvaClientGetData.create(getStructure);
                pvaClientGetData.setMessagePrefix(pvaClientChannel.getChannel().getChannelName());
            } else {
                String message = "PvaClientPutGet::channelPutGetConnect"
                        + "\npvRequest\n" + pvRequest
                        + "\nerror\n" + status.getMessage();
                      channelPutGetConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,message,null);
            }
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutGetRequester!=null) pvaClientPutGetRequester.channelPutGetConnect(status,this);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutGetRequester#putGetDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPutGet, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void putGetDone(
            Status status,
            ChannelPutGet channelPutGet,
            PVStructure getPVStructure,
            BitSet getBitSet)
    {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) System.out.println(
                "PvaClientPutGet::channelPutGetDone"
                + " channelName " + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        lock.lock();
        try { 
            channelPutGetStatus = status;
            putGetState = PutGetState.putGetComplete;
            if(status.isOK()) {
                pvaClientGetData.setData(getPVStructure,getBitSet);
            }
            waitForPutGet.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutGetRequester!=null) pvaClientPutGetRequester.putGetDone(status,this);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutGetRequester#getPutDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPutGet, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void getPutDone(
            Status status,
            ChannelPutGet channelPutGet,
            PVStructure putPVStructure,
            BitSet putBitSet)
    {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) System.out.println(
                "PvaClientPutGet::channelGetPutDone"
                + " channelName " + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        lock.lock();
        try {
            channelPutGetStatus = status;
            putGetState = PutGetState.putGetComplete;
            if(status.isOK()) {
                PVStructure pvs = pvaClientPutGetData.getPVStructure();
                convert.copyStructure(putPVStructure,pvs);
                BitSet bs = pvaClientPutGetData.getChangedBitSet();
                bs.clear();
                bs.or(putBitSet);
            }
            waitForPutGet.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutGetRequester!=null) pvaClientPutGetRequester.getPutDone(status,this);
    }

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelPutGetRequester#getGetDone(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.ChannelPutGet, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.misc.BitSet)
     */
    @Override
    public void getGetDone(
            Status status,
            ChannelPutGet channelPutGet,
            PVStructure getPVStructure,
            BitSet getBitSet)
    {
        if(isDestroyed) return;
        if(PvaClient.getDebug()) System.out.println(
                "PvaClientPutGet::channelGetGetDone"
                + " channelName " + pvaClientChannel.getChannel().getChannelName()
                + " status " + status);
        lock.lock();
        try {
            channelPutGetStatus = status;
            putGetState = PutGetState.putGetComplete;
            if(status.isOK()) {
                pvaClientGetData.setData(getPVStructure,getBitSet);
            }
            waitForPutGet.signal();
        } finally {
            lock.unlock();
        }
        if(pvaClientPutGetRequester!=null) pvaClientPutGetRequester.getGetDone(status,this);
    }



    /**
     * clean up resources used.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientPutGet::destroy");
        synchronized (this) {
            if(isDestroyed) {
                return;
            }
            isDestroyed = true;
        }
        if(channelPutGet!=null) channelPutGet.destroy();
    }

    /**
     * call issueConnect and then waitConnect.
     * @throws RuntimeException if create fails.
     */
    public void connect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::connect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueConnect();
        Status status = waitConnect();
        if(status.isOK()) return;
        String message = "channel " + pvaClientChannel.getChannel().getChannelName()
        + " PvaClientPut::connect " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * create the channelPutGet connection to the pvaClientChannel.getChannel().
     * This can only be called once.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::issueConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState!=PutGetConnectState.connectIdle) {
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
            + "  pvaClientPutGet already connected";
            throw new RuntimeException(message);
        }
        connectState = PutGetConnectState.connectActive;
        channelPutGetConnectStatus = StatusFactory.getStatusCreate().createStatus(Status.StatusType.ERROR,"connect active",null);
        channelPutGet = pvaClientChannel.getChannel().createChannelPutGet(this, pvRequest);
    }

    /**
     * wait until the channelGet connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::waitConnect()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(connectState==PutGetConnectState.connected) {
                if(!channelPutGetConnectStatus.isOK()) connectState = PutGetConnectState.connectIdle;
                return channelPutGetConnectStatus;
            }
            if(connectState!=PutGetConnectState.connectActive) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " pvaClientGet illegal connect state ";
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
            if(!channelPutGetConnectStatus.isOK()) connectState = PutGetConnectState.connectIdle;
        } finally {
            lock.unlock();
        }
        return channelPutGetConnectStatus;
    }

    /**
     * Call issuePutGet and then waitGet.
     * @throws RuntimeException if putGet fails..
     */
    public void putGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::putGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issuePutGet();
        Status status = waitPutGet();
        if(status.isOK()) return;
        String message = "channel " + pvaClientChannel.getChannel().getChannelName()
        + " PvaClientPut::get " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a putGet and return immediately.
     */
    public void issuePutGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::issuePutGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==PutGetConnectState.connectIdle) connect();
        if(putGetState==PutGetState.putGetActive){
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
            + " PvaClientPutGet::issuePutGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putGetState = PutGetState.putGetActive;
        channelPutGet.putGet(pvaClientPutGetData.getPVStructure(),pvaClientPutGetData.getChangedBitSet());
    }
    /**
     * Wait until putGet completes or for timeout.
     * @return status of putGet request.
     */
    public Status waitPutGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::waitPutGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(putGetState==PutGetState.putGetComplete) return channelPutGetStatus;
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        +  " PvaClientGetGet::waitPutGet llegal putGet state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            return channelPutGetStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Call issueGetGet and then waitGet.
     * @throws RuntimeException if getGet fails.
     */
    public void getGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::getGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueGetGet();
        Status status = waitGetGet();
        if(status.isOK()) return;
        String message = "channel " + pvaClientChannel.getChannel().getChannelName()
        + " PvaClientPutGet::getGet " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a getGet and return immediately.
     */
    public void issueGetGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::issueGetGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }

        if(connectState==PutGetConnectState.connectIdle) connect();
        if(putGetState==PutGetState.putGetActive){
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
            + " PvaClientPutGet::issueGetGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putGetState = PutGetState.putGetActive;
        channelPutGet.getGet();
    }
    /**
     * Wait until get completes or for timeout.
     * @return status of getGet request.
     */
    public Status waitGetGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::waitGetGet()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(putGetState==PutGetState.putGetComplete) return channelPutGetStatus;
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        +  " PvaClientPutGet::waitGetGet llegal get state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            return channelPutGetStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Call issuePut and then waitPut.
     * @throws RuntimeException if getGet fails.
     */
    public void getPut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::getPut()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        issueGetPut();
        Status status = waitGetPut();
        if(status.isOK()) return;
        String message = "channel " + pvaClientChannel.getChannel().getChannelName()
        + " PvaClientPut::getPut " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Call getPut and return.
     */
    public void issueGetPut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::issueGetPut()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        if(connectState==PutGetConnectState.connectIdle) connect();
        if(putGetState==PutGetState.putGetActive){
            String message = "channel " + pvaClientChannel.getChannel().getChannelName()
            + " PvaClientPutGet::issueGetPut get or put aleady active ";
            throw new RuntimeException(message);
        }
        putGetState = PutGetState.putGetActive;
        channelPutGet.getPut();
    }

    /**
     * Wait until put completes.
     * @return status of getPut request.
     */
    public Status waitGetPut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::waitGetPut()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        lock.lock();
        try {
            if(putGetState==PutGetState.putGetComplete) return channelPutGetStatus;
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        +  " PvaClientPutGet::waitGetPut llegal put state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + pvaClientChannel.getChannel().getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            return channelPutGetStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the data
     * @return The interface.
     */
    public PvaClientGetData getGetData()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::getGetData()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        checkPutGetState();
        if(putGetState==PutGetState.putGetIdle) {
            getGet();
            getPut();
        }
        return pvaClientGetData;
    }

    /**
     * Get the data
     * @return The interface.
     */
    public PvaClientPutData getPutData()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::getPutData()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        checkPutGetState();
        if(putGetState==PutGetState.putGetIdle) {
            getGet();
            getPut();
        }
        return pvaClientPutGetData;
    }  
    /**
     * Set a user callback.
     * @param pvaClientPutGetRequester The requester which must be implemented by the caller.
     */
    public void setRequester(PvaClientPutGetRequester pvaClientPutGetRequester)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientPutGet::setRequester()"
                 + " channelName " +  pvaClientChannel.getChannel().getChannelName());
        }
        this.pvaClientPutGetRequester = pvaClientPutGetRequester;
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

