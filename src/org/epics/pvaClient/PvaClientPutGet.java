/**
 * 
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
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
 *
 */
public class PvaClientPutGet implements ChannelPutGetRequester
{

    /**
     * Create new PvaClientPutGet.
     * @param pvaClient The pvaClient
     * @param channel The channel.
     * @param pvRequest The pvRequest.
     * @return The interface.
     */
    static public PvaClientPutGet create(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientPutGet(pvaClient,channel,pvRequest);
    }

    private PvaClientPutGet(
            PvaClient pvaClient,
            Channel channel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.channel = channel;
        this.pvRequest = pvRequest;
        if(PvaClient.getDebug()) System.out.println("PvaClientPutGet::PvaClientPutGet");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();

    private enum PutGetConnectState {connectIdle,connectActive,connected};
    private final PvaClient pvaClient;
    private final Channel channel;
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


    private enum PutGetState {putGetIdle,putGetActive,putGetComplete};
    private volatile PutGetState putGetState = PutGetState.putGetIdle;

    void checkPutGetState()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGetGet was destroyed");
        if(connectState==PutGetConnectState.connectIdle)
        {
            connect();
            getPut();
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
        if(isDestroyed) throw new RuntimeException("pvaClientPutGetGet was destroyed");
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
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");       
        lock.lock();
        try {
            if(PvaClient.getDebug()) System.out.println(
                    "PvaClientPutGet::channelPutGetConnect"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
            channelPutGetConnectStatus = status;
            connectState = PutGetConnectState.connected;
            this.channelPutGet = channelPutGet;
            if(status.isOK()) {
                pvaClientPutGetData = PvaClientPutData.create(putStructure);
                pvaClientPutGetData.setMessagePrefix(channel.getChannelName());
                pvaClientGetData = PvaClientGetData.create(getStructure);
                pvaClientGetData.setMessagePrefix(channel.getChannelName());
            }
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
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
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        lock.lock();
        try {
            if(PvaClient.getDebug()) System.out.println(
                    "PvaClientPutGet::channelPutGetDone"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
            channelPutGetStatus = status;
            putGetState = PutGetState.putGetComplete;
            if(status.isOK()) {
                pvaClientGetData.setData(getPVStructure,getBitSet);
            }
            waitForPutGet.signal();
        } finally {
            lock.unlock();
        }
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
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        lock.lock();
        try {
            if(PvaClient.getDebug()) System.out.println(
                    "PvaClientPutGet::channelGetPutDone"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
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
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        lock.lock();
        try {
            if(PvaClient.getDebug()) System.out.println(
                    "PvaClientPutGet::channelGetGetDone"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
            channelPutGetStatus = status;
            putGetState = PutGetState.putGetComplete;
            if(status.isOK()) {
                pvaClientGetData.setData(getPVStructure,getBitSet);
            }
            waitForPutGet.signal();
        } finally {
            lock.unlock();
        }
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
        issueConnect();
        Status status = waitConnect();
        if(status.isOK()) return;
        String message = "channel " + channel.getChannelName()
        + " PvaClientPut::connect " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * create the channelPutGet connection to the channel.
     * This can only be called once.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(connectState!=PutGetConnectState.connectIdle) {
            String message = "channel " + channel.getChannelName()
            + "  pvaClientPutGet already connected";
            throw new RuntimeException(message);
        }
        connectState = PutGetConnectState.connectActive;
        channelPutGet = channel.createChannelPutGet(this, pvRequest);
    }

    /**
     * wait until the channelGet connection to the channel is complete.
     * @return status of connection request.
     */
    public Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        lock.lock();
        try {
            if(connectState==PutGetConnectState.connected) {
                if(!channelPutGetConnectStatus.isOK()) connectState = PutGetConnectState.connectIdle;
                return channelPutGetConnectStatus;
            }
            if(connectState!=PutGetConnectState.connectActive) {
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
            if(!channelPutGetConnectStatus.isOK()) connectState = PutGetConnectState.connectIdle;
            return channelPutGetConnectStatus;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Call issuePutGet and then waitGet.
     * @throws RuntimeException if putGet fails..
     */
    public void putGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        issuePutGet();
        Status status = waitPutGet();
        if(status.isOK()) return;
        String message = "channel " + channel.getChannelName()
        + " PvaClientPut::get " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a putGet and return immediately.
     */
    public void issuePutGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(connectState==PutGetConnectState.connectIdle) connect();
        if(putGetState!=PutGetState.putGetIdle){
            String message = "channel " + channel.getChannelName()
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
        lock.lock();
        try {
            if(putGetState==PutGetState.putGetComplete) {
                putGetState = PutGetState.putGetIdle;
                return channelPutGetStatus;
            }
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientGetGet::waitPutGet llegal putGet state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            putGetState = PutGetState.putGetIdle;
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
        issueGetGet();
        Status status = waitGetGet();
        if(status.isOK()) return;
        String message = "channel " + channel.getChannelName()
        + " PvaClientPutGet::getGet " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Issue a getGet and return immediately.
     */
    public void issueGetGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(connectState==PutGetConnectState.connectIdle) connect();
        if(putGetState!=PutGetState.putGetIdle){
            String message = "channel " + channel.getChannelName()
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
        lock.lock();
        try {
            if(putGetState==PutGetState.putGetComplete) {
                putGetState = PutGetState.putGetIdle;
                return channelPutGetStatus;
            }
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientPutGet::waitGetGet llegal get state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            putGetState = PutGetState.putGetIdle;
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
        issueGetPut();
        Status status = waitGetPut();
        if(status.isOK()) return;
        String message = "channel " + channel.getChannelName()
        + " PvaClientPut::put " + status.getMessage();
        throw new RuntimeException(message);
    }

    /**
     * Call getPut and return.
     */
    public void issueGetPut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPutGet was destroyed");
        if(connectState==PutGetConnectState.connectIdle) connect();
        if(putGetState!=PutGetState.putGetIdle){
            String message = "channel " + channel.getChannelName()
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
        lock.lock();
        try {
            if(putGetState==PutGetState.putGetComplete) {
                putGetState = PutGetState.putGetIdle;
                return channelPutGetStatus;
            }
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientPutGet::waitGetPut llegal put state ";
                throw new RuntimeException(message);
            }
            try {
                waitForPutGet.await();
            } catch(InterruptedException e) {
                String message = "channel "
                        + channel.getChannelName() 
                        + " InterruptedException " + e.getMessage();
                throw new RuntimeException(message);
            }
            putGetState = PutGetState.putGetIdle;
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
        checkPutGetState();
        return pvaClientGetData;
    }

    /**
     * Get the data
     * @return The interface.
     */
    public PvaClientPutData getPutData()
    {
        checkPutGetState();
        return pvaClientPutGetData;
    }  
}

