/* PvaClientMultiChannel.java */
/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
/**
 * @author mrk
 * @date 2015.07
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.Channel.ConnectionState;
import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;

/**
 * Provides access to multiple channels.
 *
 * @author mrk
 */
public class PvaClientMultiChannel
{
    /** Create a PvaClientMultiChannel with provider = "pva" and maxNotConnected = 0.
     * @param pvaClient The interface to pvaClient.
     * @param channelNames The names of the channels.
     * @return The interface to the PvaClientMultiChannel
     */
    static public PvaClientMultiChannel create(
            PvaClient pvaClient,
            String[] channelNames
            )
    {
        return create(pvaClient,channelNames,"pva",0);
    }
    
    private PvaClientMultiChannel(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName,
            int maxNotConnected)
    {
        this.pvaClient = pvaClient;
        this.channelName = channelNames;
        this.providerName = providerName;
        this.maxNotConnected = maxNotConnected;
        numChannel = channelName.length;
        pvaClientChannelArray = new PvaClientChannel[numChannel];
        isConnected = new boolean[numChannel];
        for(int i=0; i<numChannel; ++i)
        {
            pvaClientChannelArray[i] = null;
            isConnected[i] = false;
        }
    }

    private void checkConnected()
    {
        if(numConnected==0) connect(3.0);
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private final CreateRequest createRequest = CreateRequest.create();
    private final PvaClient pvaClient;
    private final String[] channelName;
    private final String providerName;
    private final int maxNotConnected;

    private final int numChannel;
    private final ReentrantLock lock = new ReentrantLock();

    private int numConnected = 0;
    private PvaClientChannel[] pvaClientChannelArray;
    boolean[] isConnected;
    boolean isDestroyed = false;

    /** Create a PvaClientMultiChannel.
     * @param pvaClient The interface to pvaClient.
     * @param channelNames The names of the channel..
     * @param providerName The name of the provider.
     * @param maxNotConnected The maximum number of channels that can be disconnected.
     * @return The interface to the PvaClientMultiChannel
     */
    static public PvaClientMultiChannel create(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName,
            int maxNotConnected
            )
    {
        return new PvaClientMultiChannel(pvaClient,channelNames,providerName,maxNotConnected);
    }

    /** Destroy the pvAccess connection.
     */
    public void destroy()
    {
        lock.lock();
        try {
            if(isDestroyed) return;
            isDestroyed = true;
        } finally {
            lock.unlock();
        }
        for(int i=0; i<numChannel; ++i) 
        {
            if(pvaClientChannelArray!=null) pvaClientChannelArray[i].destroy();
            pvaClientChannelArray[i] = null;
        }
        pvaClientChannelArray = null;
    }
    /** Get the channelNames.
     * @return The names.
     */
    public String[] getChannelNames()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        return channelName;
    }
    /** Connect to the channels with timeout = 5.0.
     * @return status of request
     */
    public Status connect()
    {
        return connect(5.0);
    }
    /** Connect to the channels.
     * This calls issueConnect and waitConnect.
     * An exception is thrown if connect fails.
     * @param timeout The time to wait for connecting to the channels.
     * @return status of request
     */
    public Status connect(double timeout)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        for(int i=0; i< numChannel; ++i) {
            pvaClientChannelArray[i] = pvaClient.createChannel(channelName[i],providerName);
            pvaClientChannelArray[i].issueConnect();
        }
        Status returnStatus = statusCreate.getStatusOK();
        Status status = statusCreate.getStatusOK();
        int numBad = 0;
        for(int i=0; i< numChannel; ++i) {
            if(numBad==0) {
                status = pvaClientChannelArray[i].waitConnect(timeout);
            } else {
                status = pvaClientChannelArray[i].waitConnect(.001);
            }
            if(status.isOK()) {
                ++numConnected;
                isConnected[i] = true;
                continue;
            }
            if(returnStatus.isOK()) returnStatus = status;
            ++numBad;
            if(numBad>maxNotConnected) break;
        }
        return numBad>maxNotConnected ? returnStatus : statusCreate.getStatusOK();
    }
    /** Are all channels connected?
     * @return if all are connected.
     */
    public boolean allConnected()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        return (numConnected==numChannel) ? true : false;
    }
    /** Has a connection state change occured?
     * @return (true, false) if (at least one, no) channel has changed state.
     */
    public boolean connectionChange()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        for(int i=0; i<numChannel; ++i) {
            PvaClientChannel pvaClientChannel = pvaClientChannelArray[i];
            Channel channel = pvaClientChannel.getChannel();
            ConnectionState stateNow = channel.getConnectionState();
            boolean connectedNow = stateNow==ConnectionState.CONNECTED ? true : false;
            if(connectedNow!=isConnected[i]) return true;
        }
        return false;
    }
    /** Get the connection state of each channel.
     * @return The state of each channel.
     */
    public boolean[] getIsConnected()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        for(int i=0; i<numChannel; ++i) {
            PvaClientChannel pvaClientChannel = pvaClientChannelArray[i];
            if(pvaClientChannel==null) {
                isConnected[i] = false;
                continue;
            }
            Channel channel = pvaClientChannel.getChannel();
            ConnectionState stateNow = channel.getConnectionState();
            isConnected[i] = (stateNow==ConnectionState.CONNECTED) ? true : false;
        }
        return isConnected;
    }
    /** Get the pvaClientChannelArray.
     * @return The interface.
     */
    public PvaClientChannel[] getPvaClientChannelArray()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        return pvaClientChannelArray;
    }
    /** Get pvaClient.
     * @return The interface.
     */
    public PvaClient getPvaClient()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        return pvaClient;
    }
    /**
     * create a pvaClientMultiGetDouble
     * @return The interface.
     */
    public PvaClientMultiGetDouble createGet()
    {
        checkConnected();
        return PvaClientMultiGetDouble.create(this,pvaClientChannelArray);
    }   
    /**
     * create a pvaClientMultiPutDouble
     * @return The interface.
     */
    public PvaClientMultiPutDouble createPut()
    {
        checkConnected();
        return PvaClientMultiPutDouble.create(this,pvaClientChannelArray);
    }
    /**
     * Create a pvaClientMultiMonitorDouble.
     * @return The interface.
     */
    public PvaClientMultiMonitorDouble createMonitor()
    {
        checkConnected();
        return PvaClientMultiMonitorDouble.create(this, pvaClientChannelArray);
    }
    /**
     * Create a pvaClientNTMultiPut.
     * @return The interface.
     */
    public PvaClientNTMultiPut createNTPut()
    {
        checkConnected();
        return PvaClientNTMultiPut.create(this, pvaClientChannelArray);   
    }
    /**
     * Create a pvaClientNTMultiGet.
     * This calls the next method with request = "value,alarm,timeStamp"
     * @return The interface.
     */
    public PvaClientNTMultiGet createNTGet()
    {
        return createNTGet("value,alarm,timeStamp");
    }
    /**
     * Create a pvaClientNTMultiGet;
     * @param request The request for each channel.
     * @return The interface.
     */
    public PvaClientNTMultiGet createNTGet(String request)
    {
        checkConnected();
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String message = " PvaClientMultiChannel::createNTGet invalid pvRequest: "
                 + createRequest.getMessage();
            throw new RuntimeException(message);
            
        }
        return PvaClientNTMultiGet.create(this, pvaClientChannelArray,pvRequest);
    }
    /**
     * Create a pvaClientNTMultiMonitor.
     * This calls the next method with request = "value,alarm,timeStamp"
     * @return The interface.
     */
    public PvaClientNTMultiMonitor createNTMonitor()
    {
        return createNTMonitor("value,alarm,timeStamp");
    }
    /**
     * Create a pvaClientNTMultiPut.
     * @param request The request for each channel.
     * @return The interface.
     */
    public PvaClientNTMultiMonitor createNTMonitor(String request)
    {
        checkConnected();
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String message = " PvaClientMultiChannel::createNTMonitor invalid pvRequest: "
                 + createRequest.getMessage();
            throw new RuntimeException(message);
            
        }
        return PvaClientNTMultiMonitor.create(this, pvaClientChannelArray,pvRequest);
    }

};

