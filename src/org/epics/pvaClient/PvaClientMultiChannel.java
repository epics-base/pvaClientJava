/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Union;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvdata.property.*;
import org.epics.pvdata.factory.*;
import org.epics.pvdata.pv.*;
import org.epics.pvdata.misc.*;
import org.epics.pvdata.monitor.*;
import org.epics.pvaccess.client.*;

/**
 * An easy to use interface to get/put data from/to multiple channels.
 * @author mrk
 *
 */
public class PvaClientMultiChannel {
    static public PvaClientMultiChannel create(
        PvaClient pvaClient,
        String[] channelNames,
        String providerName)
    {
         return new PvaClientMultiChannel(pvaClient,channelNames,providerName);
    }

    public PvaClientMultiChannel(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName)
    {
        this.pvaClient = pvaClient;
        this.channelName = channelNames;
        this.providerName = providerName;
        numChannel = channelNames.length;
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    private final PvaClient pvaClient;
    private final String[] channelName;
    private final String providerName;
    private final int numChannel;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();

    private volatile int numConnected = 0;
    private volatile PvaClientChannel[] pvaClientChannelArray = null;
    private volatile boolean[] isConnected = null;

    private volatile boolean isDestroyed = false;
    private volatile Status[] channelStatus = null;

    
     /**
      * Destroy all resources.
      */
     public void destroy()
     {
         synchronized (this) {
             if(isDestroyed) return;
             isDestroyed = true;
         }
         if(pvaClientChannelArray==null) return;
         for(int i=0; i< pvaClientChannelArray.length; ++i) {
             if(pvaClientChannelArray[i]!=null) pvaClientChannelArray[i].destroy();
         }
     }
      /**
      * Get the channelNames.
      * @return the names.
      */
     public String[] getChannelNames()
     {
         return channelName;
     }
     /** Connect to the channels.
     * This calls issueConnect and waitConnect.
     * An exception is thrown if connect fails.
     * @param timeout The time to wait for connecting to the channel.
     * @param maxNotConnected Maximum number of channels that do not connect.
     * @return status of request
     */
     public Status connect(double timeout,int maxNotConnected)
     {
         if(isDestroyed) {
             throw new RuntimeException("pvaClientMultiChannel was destroyed");
         }
         if(pvaClientChannelArray!=null) {
             throw new RuntimeException("pvaClientMultiChannel already connected");
         }
         if(pvaClient==null) {
             throw new RuntimeException("pvaClientMultiChannel pvaClient is gone");
         }
         pvaClientChannelArray = new PvaClientChannel[numChannel];
         isConnected = new boolean[numChannel];
         for(int i=0; i<numChannel; ++i)
         {
             isConnected[i] = false;
         }
         for(int i=0; i<numChannel; ++i)
         {
             pvaClientChannelArray[i] = pvaClient.createChannel(channelName[i],providerName);
             pvaClientChannelArray[i].issueConnect();
         }
         Status returnStatus = statusCreate.getStatusOK();
         Status status = statusCreate.getStatusOK();
         int numBad = 0;
         for(int  i=0; i< numChannel; ++i) {
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
         if(isDestroyed) {
             throw new RuntimeException("pvaClientMultiChannel was destroyed");
         }
         if(pvaClientChannelArray==null) {
             throw new RuntimeException("pvaClientMultiChannel not connected");
         }
         return (numConnected==numChannel) ? true : false;
     }
    /** Has a connection state change occured?
     * @return (true, false) if (at least one, no) channel has changed state.
     */
    public boolean connectionChange()
    {
         if(isDestroyed) {
             throw new RuntimeException("pvaClientMultiChannel was destroyed");
         }
         if(pvaClientChannelArray==null) {
             throw new RuntimeException("pvaClientMultiChannel not connected");
         }
         if(numConnected==numChannel) return true;
         for(int i=0; i<numChannel; ++i) {
             PvaClientChannel pvaClientChannel = pvaClientChannelArray[i];
             Channel channel = pvaClientChannel.getChannel();
             Channel.ConnectionState stateNow = channel.getConnectionState();
             boolean connectedNow = stateNow==Channel.ConnectionState.CONNECTED ? true : false;
             if(connectedNow!=isConnected[i]) return true;
        }
        return false; 
    }

    /** Get the connection state of each channel.
     * @return The state of each channel.
     */
    boolean[] getIsConnected()
    {
        if(isDestroyed) {
             throw new RuntimeException("pvaClientMultiChannel was destroyed");
        }
        if(pvaClientChannelArray==null) {
            throw new RuntimeException("pvaClientMultiChannel not connected");
        }
        for(int i=0; i<numChannel; ++i) isConnected[i] = false;
        PvaClientChannel[] channels = pvaClientChannelArray;
        for(int i=0; i<numChannel; ++i) {
            PvaClientChannel pvaClientChannel = channels[i];
            Channel channel = pvaClientChannel.getChannel();
            Channel.ConnectionState stateNow = channel.getConnectionState();
            if(stateNow==Channel.ConnectionState.CONNECTED) isConnected[i] = true;
        }
        return isConnected;
    }
    /** Get the pvaClientChannelArray.
     * @return The weak shared pointer.
     */
    PvaClientChannel[] getPvaClientChannelArray()
    {
        if(isDestroyed) {
             throw new RuntimeException("pvaClientMultiChannel was destroyed");
        }
        if(pvaClientChannelArray==null) {
            throw new RuntimeException("pvaClientMultiChannel not connected");
        }
        return pvaClientChannelArray;
    }
    /** Get pvaClient.
     * @return The weak shared pointer.
     */
    PvaClient getPvaClient()
    {
        if(isDestroyed) {
             throw new RuntimeException("pvaClientMultiChannel was destroyed");
        }
        return pvaClient;
    }
     

}
