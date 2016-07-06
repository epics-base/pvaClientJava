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
import org.epics.pvdata.pv.Status.StatusType;
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
        if(PvaClient.getDebug()) System.out.println("PvaClientPut::PvaClientPut");
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();

    private enum PutConnectState {connectIdle,connectActive,connected};
    private final PvaClient pvaClient;
    private final Channel channel;
    private final PVStructure pvRequest;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();
    private final Condition waitForPutGet = lock.newCondition();
    private PvaClientPutData pvaClientData = null;

    private volatile boolean isDestroyed = false;
    private volatile Status channelPutConnectStatus = statusCreate.getStatusOK();
    private volatile Status channelGetPutStatus = statusCreate.getStatusOK();
    private volatile ChannelPut channelPut = null;
    private volatile PutConnectState connectState = PutConnectState.connectIdle;

    private enum PutGetState {putGetIdle,putGetActive,putGetComplete};
    private volatile PutGetState putGetState = PutGetState.putGetIdle;
   
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
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientPut::clientPutConnect"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
            }
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
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientPut::getDone"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
            }
            channelGetPutStatus = status;
            if(status.isOK()) {
                PVStructure pvs = pvaClientData.getPVStructure();
                convert.copyStructure(pvStructure,pvs);
                BitSet bs = pvaClientData.getChangedBitSet();
                bs.clear();
                bs.or(bitSet);
            }
            putGetState = PutGetState.putGetComplete;
            waitForPutGet.signal();
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
            if(PvaClient.getDebug()) {
                System.out.println("PvaClientPut::putDone"
                    + " channelName " + channel.getChannelName()
                    + " status " + status);
            }
            channelGetPutStatus = status;
            putGetState = PutGetState.putGetComplete;
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
        issueConnect();
        Status status = waitConnect();
        if(status.isOK()) return;
        String message = "channel " 
             + channel.getChannelName()
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
        String message = "channel " 
            + channel.getChannelName()
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
        if(connectState==PutConnectState.connectIdle) connect();
        if(putGetState!=PutGetState.putGetIdle){
            String message = "channel " + channel.getChannelName()
            + " PvaClientPut::issueGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putGetState = PutGetState.putGetActive;
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
            if(putGetState==PutGetState.putGetComplete) {
                putGetState = PutGetState.putGetIdle;
                return channelGetPutStatus;
            }
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientPut::waitGet llegal putGet state ";
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
        String message = "channel " 
            + channel.getChannelName()
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
        if(connectState==PutConnectState.connectIdle) connect();
        if(putGetState!=PutGetState.putGetIdle){
            String message = "channel " + channel.getChannelName()
            + " PvaClientPut::issueGet get or put aleady active ";
            throw new RuntimeException(message);
        }
        putGetState = PutGetState.putGetActive;
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
            if(putGetState==PutGetState.putGetComplete) {
                putGetState = PutGetState.putGetIdle;
                return channelGetPutStatus;
            }
            if(putGetState!=PutGetState.putGetActive){
                String message = "channel "
                        + channel.getChannelName() 
                        +  " PvaClientPut::waitPut llegal putGet state ";
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
