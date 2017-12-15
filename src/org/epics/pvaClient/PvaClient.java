/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.ChannelProviderRegistry;
import org.epics.pvaccess.client.ChannelProviderRegistryFactory;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.Requester;

/**
 * PvaClient is a synchronous  interface to pvAccess.
 * @author mrk
 * @since 2015.06
 */
public class PvaClient implements Requester {
    /**
     * Get the single instance of PvaClient.
     * @param providerNames - space separated list of provider names.
     * @return The interface to PvaClient.
     */
    static public synchronized PvaClient get(String providerNames) {
        if(pvaClient==null) {
            ChannelProviderRegistry registry = ChannelProviderRegistryFactory.getChannelProviderRegistry();
            pvaClient = new PvaClient();
            String[] names = providerNames.split("\\s+");
            for (String name : names)
            {
                if(name.equals("pva")) {
                    org.epics.pvaccess.ClientFactory.start();
                    pvaClient.pvaStarted = true;
                } else if(name.equals("ca")) {
                    org.epics.ca.ClientFactory.start();
                    pvaClient.caStarted = true;
                } else {
                    if(registry.getProvider(name)==null) {
                        System.err.println("PvaClient::get provider " + name  + " not known");
                    }
                }
            }
        }
        return pvaClient;
    }
    /**
     * Get the single instance of PvaClient.
     * This connects to providers pva and ca.
     * @return The interface to PvaClient.
     */
    static public synchronized PvaClient get() {
        return get("pva ca");
    }

    private static PvaClient pvaClient = null;
    private static boolean debug = false;
    private boolean pvaStarted = false;
    private boolean caStarted = false;
    private static final PvaClientChannelCache pvaClientChannelCache = new PvaClientChannelCache();
    private static final String pvaClientName = "pvaClient";
    private static final String defaultProvider =
            org.epics.pvaccess.ClientFactory.PROVIDER_NAME;
    private Requester requester = null;
    private boolean isDestroyed = false;

    static private class PvaClientChannelCache
    {
        public PvaClientChannelCache(){}
        
        void destroy() {
        	Set<String> names = pvaClientChannelMap.keySet();
        	Iterator<String> iter = names.iterator();
        	while(iter.hasNext()) {
        		PvaClientChannel pvaChannel = pvaClientChannelMap.get(iter.next());
        		pvaChannel.destroy();
        	}
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
            if(pvaClientChannelMap.get(name)!=null) {
                throw new RuntimeException("pvaClientChannelCache::addChannel channel already cached");
            }
            pvaClientChannelMap.put(name, pvaClientChannel);
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
                result += "channel " + channelName + " providerName " + providerName + "\n";
                result += "  get and put cacheSize " + pvaChannel.cacheSize() + "\n";
                result += "  get and put cache\n" + pvaChannel.showCache();
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
     
    

    /**
     * Destroy all cached channels.
     */
    public void destroy()
    {
        synchronized (this) {
            if(isDestroyed) {
                System.out.println("Why was PvaClient::destroy() called more then once????");
                return;
            }
            isDestroyed = true;
        }
        if(PvaClient.getDebug()) System.out.println(pvaClientChannelCache.toString());
        pvaClientChannelCache.destroy();
        if(pvaStarted) {
            System.out.println("calling org.epics.pvaccess.ClientFactory::stop()");
            org.epics.pvaccess.ClientFactory.stop();
        }
        if(caStarted) {
            System.out.println("calling org.epics.ca.ClientFactory::stop()");
            org.epics.ca.ClientFactory.stop();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#getRequesterName()
     */
    public  String getRequesterName() {
        if(requester!=null) return requester.getRequesterName();
        return pvaClientName;
    }

    /* (non-Javadoc)
     * @see org.epics.pvdata.pv.Requester#message(java.lang.String, org.epics.pvdata.pv.MessageType)
     */
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
     * @param channelName The channelName.
     * @return The interface.
     * @throws RuntimeException if connection fails.
     */
    public PvaClientChannel channel(String channelName)
    {
        return channel(channelName,"pva",5.0);
    }
    
    /**
     * Get a cached channel or create and connect to a new channel.
     * The timeout is 5 seconds.
     * @param channelName The channelName.
     * @param providerName The provider name.
     * @return The interface.
     * @throws RuntimeException if connection fails.
     */
    public PvaClientChannel channel(
            String channelName,
            String providerName)
    {
        return channel(channelName,providerName,5.0);
    }

    /**
     * Get a cached channel or create and connect to a new channel.
     * @param channelName The channelName.
     * @param providerName The provider name.
     * @param timeOut The number of seconds to wait for connection. 0.0 means forever.
     * @return The interface.
     * @throws RuntimeException if connection fails.
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
     * @throws RuntimeException if connection fails.
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
     * @throws RuntimeException if connection fails.
     * @return The channel.
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
     * @return A String showing the cached channels.
     */
    public String showCache()
    {
         return pvaClientChannelCache.toString();
    }
     /** Get the number of cached gets and puts.
      * @return The size.
     */
    public int cacheSize()
    {
         return pvaClientChannelCache.cacheSize();
    }
    /** Should debug info be shown?
     * @param value true or false
     */
    public static void setDebug(boolean value) {debug = value;}
    /** Is debug set?
     * @return true or false
     */
    public static boolean getDebug() {return debug;}
}
