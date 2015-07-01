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
 * An easy interface to channelPut.
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
            PvaClientChannel pvaClientChannel,
            Channel channel,
            PVStructure pvRequest)
    {
        return new PvaClientPut(pvaClient,pvaClientChannel,channel,pvRequest);
    }

    public PvaClientPut(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            Channel channel,
            PVStructure pvRequest)
    {
        this.pvaClient = pvaClient;
        this.pvaClientChannel = pvaClientChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();

    private enum PutConnectState {connectIdle,connectActive,connected};
    private final PvaClient pvaClient;
    private final PvaClientChannel pvaClientChannel;
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

    @Override
    public String getRequesterName() {
        return pvaClient.getRequesterName();
    }
    @Override
    public void message(String message, MessageType messageType) {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        pvaClient.message(message, messageType);
    }
    @Override
    public void channelPutConnect(
        Status status,
        ChannelPut channelPut,
        Structure structure)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        channelPutConnectStatus = status;
        this.channelPut = channelPut;
        if(!status.isOK()) {
            pvaClientData = PvaClientPutData.create(structure);
            pvaClientData.setMessagePrefix(pvaClientChannel.getChannelName());
        }

        lock.lock();
        try {
            waitForConnect.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void getDone(
        Status status,
        ChannelPut channelPut,
        PVStructure pvStructure,
        BitSet bitSet)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        channelGetPutStatus = status;
        if(status.isOK()) {
            PVStructure pvs = pvaClientData.getPVStructure();
            convert.copyStructure(pvStructure,pvs);
            BitSet bs = pvaClientData.getBitSet();
            bs.clear();
            bs.or(bitSet);
        }
        lock.lock();
        try {
            waitForPutOrGet.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putDone(Status status, ChannelPut channelPut) {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        channelGetPutStatus = status;
        lock.lock();
        try {
            waitForPutOrGet.signal();
        } finally {
            lock.unlock();
        }
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
        if(channelPut!=null) channelPut.destroy();
    }

    /**
     * call issueConnect and then waitConnect.
     */
    void connect()
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
     * create the channelGet connection to the channel.
     * This can only be called once.
     */
    void issueConnect()
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
     * wait until the channelGet connection to the channel is complete.
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    Status waitConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState!=PutConnectState.connectActive)
        {
            String message = "channel " + channel.getChannelName()
                + "  pvaClientPut illegal connect state";
            throw new RuntimeException(message);
        }
        try {
            lock.lock();
            try {
                waitForConnect.await();
            } catch(InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        } finally {
            lock.unlock();
        }
        if(channelPutConnectStatus.isOK()) {
            connectState = PutConnectState.connected;
            return statusCreate.getStatusOK();
        }
        return statusCreate.createStatus(
                StatusType.ERROR,
                channelPutConnectStatus.getMessage(),
                null);
    }

    /**
     * Call issueGet and then waitGet.
     * @return (false,true) means (failure,success)
     */
    void get()
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
    void issueGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState==PutConnectState.connectIdle) connect();
        if(putState!=PutState.putIdle){
             String message = "channel " + channel.getChannelName()
                 + " PvaClientPut::issueGet get or put aleady active ";
             throw new RuntimeException(message);
        }
        putState = PutState.getActive;
        pvaClientData.getBitSet().clear();
        channelPut.get();
    }
    /**
     * Wait until get completes or for timeout.
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    Status waitGet()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(putState!=PutState.getActive){
            String message = "channel " + channel.getChannelName()
                    + " PvaClientPut::issueGet illegal put state";
            throw new RuntimeException(message);
        }
        try {
            lock.lock();
            try {
                waitForPutOrGet.await();
            } catch(InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        } finally {
            lock.unlock();
        }
        putState = PutState.putIdle;
        if(channelGetPutStatus.isOK()) {
            return statusCreate.getStatusOK();
        }
        return statusCreate.createStatus(
                StatusType.ERROR,
                channelPutConnectStatus.getMessage(),
                null);
    }

    /**
     * Call issuePut and then waitPut.
     * @return The result of waitPut.
     */
    void put()
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
    void issuePut()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(connectState==PutConnectState.connectIdle) connect();
        if(putState!=PutState.putIdle){
             String message = "channel " + channel.getChannelName()
                 + " PvaClientPut::issueGet get or put aleady active ";
             throw new RuntimeException(message);
        }
        putState = PutState.putActive;
        channelPut.put(pvaClientData.getPVStructure(),pvaClientData.getBitSet());
    }

    /**
     * Wait until put completes.
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    Status waitPut()
    {
         if(isDestroyed) throw new RuntimeException("pvaClientPut was destroyed");
        if(putState!=PutState.putActive){
            String message = "channel " + channel.getChannelName()
                    + " PvaClientPut::waitPut illegal put state";
            throw new RuntimeException(message);
        }
        try {
            lock.lock();
            try {
                waitForPutOrGet.await();
            } catch(InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        } finally {
            lock.unlock();
        }
        putState = PutState.putIdle;
        if(channelGetPutStatus.isOK()) {
            return statusCreate.getStatusOK();
        }
        return statusCreate.createStatus(
                StatusType.ERROR,
                channelPutConnectStatus.getMessage(),
                null);
    }

    /**
     * Get the data
     * @return The interface.
     */
    PvaClientPutData getData()
    {
        checkPutState();
        return pvaClientData;
    }  
}
