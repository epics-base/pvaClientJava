/*pvaClient.java*/
/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
/**
 * @author mrk
 * @date 2015.06
 */

package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.*;
import java.util.Collection;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.Channel.ConnectionState;
import org.epics.pvaccess.client.ChannelProvider;
import org.epics.pvaccess.client.ChannelProviderRegistryFactory;
import org.epics.pvaccess.client.ChannelRequester;

import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.LinkedList;
import org.epics.pvdata.misc.LinkedListCreate;
import org.epics.pvdata.misc.LinkedListNode;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Requester;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Union;

/**
 * PvaClient is an easy to use interface to pvAccess.
 * @author mrk
 *
 */
public class PvaClient implements Requester {

    /**
     * Create an instance of PvaClient.
     * @return The newly created PvaClient.
     */
    static public synchronized PvaClient get() {
        if(pvaClient==null) {
            pvaClient = new PvaClient();
            org.epics.pvaccess.ClientFactory.start();
            org.epics.ca.ClientFactory.start();
        }
        return pvaClient;
    }

    private static PvaClient pvaClient = null;

    static private class PvaClientChannelCache
    {
        public PvaClientChannelCache(){}
        
       
        void destroy() {
            pvaClientChannelMap.clear();
        }
        PvaClientChannel getChannel(
                String channelName,
                String providerName)
        {
            String name = channelName + providerName;
            return pvaClientChannelMap.get(name);
        }
        void addChannel(PvaClientChannel pvaClientChannel)
        {
            Channel channel = pvaClientChannel.getChannel();
            String name = channel.getChannelName() + channel.getProvider().getProviderName();
            if(pvaClientChannelMap.get(name)!=null) return;
            pvaClientChannelMap.put(name, pvaClientChannel);
        }
        void removeChannel(
                String channelName,
                String providerName)
        {
            String name = channelName + providerName;
            pvaClientChannelMap.remove(name);
        }
        public String toString()
        {
            String result = "";
            Set<String> names = pvaClientChannelMap.keySet();
            Iterator<String> iter = names.iterator();
            while(iter.hasNext()) {
                PvaClientChannel pvaChannel = pvaClientChannelMap.get(iter.next());
                
                Channel channel = pvaChannel.getChannel();
                String channelName = channel.getChannelName();
                String providerName = channel.getProvider().getProviderName();
                result += "channel " + channelName + " providerName " + providerName;
                result += "  get and put cacheSize " + pvaChannel.cacheSize() + "\n";
            }
            return result;
        }
        int cacheSize()
        {
            return pvaClientChannelMap.size();
        }
        private Map<String,PvaClientChannel> pvaClientChannelMap
             = new TreeMap<String,PvaClientChannel>();
    }
     
    private static final PvaClientChannelCache pvaClientChannelCache
        = new PvaClientChannelCache();
    private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    private static final String pvaClientName = "pvaClient";
    private static final String defaultProvider =
            org.epics.pvaccess.ClientFactory.PROVIDER_NAME;
    private static final Union variantUnion = 
            FieldFactory.getFieldCreate().createVariantUnion();

    private static PVStructure createRequest(String request)
    {
        CreateRequest factory = CreateRequest.create();
        PVStructure pvStructure = factory.createRequest(request);
        if (pvStructure == null) 
            throw new RuntimeException("invalid pvRequest: " + factory.getMessage());
        else
            return pvStructure;
    }
    private enum ConnectState {connectIdle,connectActive,notConnected,connected};

    private Requester requester = null;
    private boolean isDestroyed = false;


    /**
     * Destroy all the channels and multiChannels.
     */
    public synchronized void destroy()
    {
        if(isDestroyed) return;
        isDestroyed = true;
        pvaClientChannelCache.destroy();
        org.epics.pvaccess.ClientFactory.stop();
        org.epics.ca.ClientFactory.stop();
    }

    public  String getRequesterName() {
        if(requester!=null) return requester.getRequesterName();
        return pvaClientName;
    }

    public void message(String message, MessageType messageType) {
        if(requester!=null) {
            requester.message(message, messageType);
            return;
        }
        System.out.printf("%s %s%n", messageType.name(),message);
    }

    /**
     * Get a cached channel or create and connect to a new channel.
     * The provider is pva. The timeout is 5 seconds.
     * If connection can not be made an exception is thrown.
     * @param channelName The channelName.
     * @return The interface.
     */
    public PvaClientChannel channel(String channelName)
    {
        return channel(channelName,"pva",5.0);
    }

    /**
     * Get a cached channel or create and connect to a new channel.
     * If connection can not be made an exception is thrown.
     * @param channelName The channelName.
     * @param providerName The provider name.
     * @param timeOut The time to wait for a connection.
     * @return The interface.
     */
    public PvaClientChannel channel(
            String channelName,
            String providerName,
            double timeOut)
    {
        PvaClientChannel pvaClientChannel = 
            pvaClientChannelCache.getChannel(channelName,providerName);
        if(pvaClientChannel!=null) return pvaClientChannel;
        pvaClientChannel = createChannel(channelName,providerName);
        pvaClientChannel.connect(timeOut);
        pvaClientChannelCache.addChannel(pvaClientChannel);
        return pvaClientChannel;
    }
    /**
     * Create an PvaClientChannel. The provider is pvAccess.
     * @param channelName The channelName.
     * @return The interface.
     */
    public PvaClientChannel createChannel(String channelName)
    {
        return createChannel(channelName,defaultProvider);
    }
    
    /**
     * Create an PvaClientChannel with the specified provider.
     * @param channelName The channelName.
     * @param providerName The provider.
     * @return The interface or null if the provider does not exist.
     */
    public PvaClientChannel createChannel(String channelName,String providerName)
    {
        if(isDestroyed) return null;
        return PvaClientChannel.create(this,channelName,providerName);
    }

    /**
     * Set a requester.
     * The default is for PvaClient to handle messages by printing to System.out.
     * @param requester The requester.
     */
    public void setRequester(Requester requester)
    {
        this.requester = requester;
    }

    /**
     * Clear the requester. PvaClientPVA will handle messages.
     */
    public void clearRequester()
    {
        requester = null;
    }
    /** Show the list of cached gets and puts.
     */
    String showCache()
    {
         return pvaClientChannelCache.toString();
    }
     /** Get the number of cached gets and puts.
     */
    int cacheSize()
    {
         return pvaClientChannelCache.cacheSize();
    }
}
