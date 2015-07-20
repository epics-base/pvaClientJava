
/*pvaClientChannel.java*/
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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.Channel.ConnectionState;
import org.epics.pvaccess.client.ChannelProvider;
import org.epics.pvaccess.client.ChannelProviderRegistryFactory;
import org.epics.pvaccess.client.ChannelRequester;
import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Requester;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;

/**
 * An synchronous alternative to directly calling the Channel methods of pvAccess.
 * @author mrk
 *
 */
public class PvaClientChannel implements ChannelRequester,Requester{

    static PvaClientChannel create(
            PvaClient pvaClient,
            String channelName,
            String providerName)
    {
        return new PvaClientChannel(pvaClient,channelName,providerName);
    }

    private PvaClientChannel(
            PvaClient pvaClient,
            String channelName,
            String providerName)
            {
        this.pvaClient = pvaClient;
        this.channelName = channelName;
        this.providerName = providerName;
            }


    static private class PvaClientGetCache
    {
        public PvaClientGetCache(){}


        void destroy() {
            pvaClientGetMap.clear();
        }

        PvaClientGet getGet(
                String request)
        {
            return pvaClientGetMap.get(request);
        }
        void addGet(String request,PvaClientGet pvaClientGet)
        {
            if(pvaClientGetMap.get(request)!=null) return;
            pvaClientGetMap.put(request, pvaClientGet);
        }
        public String toString()
        {
            String result = "";
            Set<String> names = pvaClientGetMap.keySet();
            Iterator<String> iter = names.iterator();
            while(iter.hasNext()) {
                String name = iter.next();
                result += "      request " + name + "\n";
            }
            return result;
        }
        int cacheSize()
        {
            return pvaClientGetMap.size();
        }
        private Map<String,PvaClientGet> pvaClientGetMap
        = new TreeMap<String,PvaClientGet>();
    }


    static private class PvaClientPutCache
    {
        public PvaClientPutCache(){}


        void destroy() {
            pvaClientPutMap.clear();
        }

        PvaClientPut getPut(
                String request)
        {
            return pvaClientPutMap.get(request);
        }
        void addPut(String request,PvaClientPut pvaClientPut)
        {
            if(pvaClientPutMap.get(request)!=null) return;
            pvaClientPutMap.put(request, pvaClientPut);
        }
        public String toString()
        {
            String result = "";
            Set<String> names = pvaClientPutMap.keySet();
            Iterator<String> iter = names.iterator();
            while(iter.hasNext()) {
                String name = iter.next();
                result += "      request " + name + "\n";
            }
            return result;
        }
        int cacheSize()
        {
            return pvaClientPutMap.size();
        }
        private Map<String,PvaClientPut> pvaClientPutMap
        = new TreeMap<String,PvaClientPut>();
    }
    
    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private enum ConnectState {connectIdle,connectActive,notConnected,connected};
    private final PvaClient pvaClient;
    private final String channelName;
    private final String providerName;
    private volatile ConnectState connectState = ConnectState.connectIdle;
    private volatile boolean isDestroyed = false;
    private CreateRequest createRequest = new CreateRequest();
    private final PvaClientGetCache pvaClientGetCache = new PvaClientGetCache();
    private final PvaClientPutCache pvaClientPutCache = new PvaClientPutCache();
    private Status channelConnectStatus = statusCreate.getStatusOK();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();

    private volatile Channel channel = null;

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRequester#channelCreated(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.Channel)
     */
    @Override
    public void channelCreated(Status status, Channel channel) {
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        if(status.isOK()) {
            this.channel = channel;
            return;
        }
        System.err.println("PvaClientChannel::channelCreated status "
                + status.getMessage() + "why??");
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRequester#channelStateChange(org.epics.pvaccess.client.Channel, org.epics.pvaccess.client.Channel.ConnectionState)
     */
    @Override
    public void channelStateChange(Channel channel,ConnectionState connectionState) {
        lock.lock();
        try {
            if(isDestroyed) return;
            boolean waitingForConnect = false;
            if(connectState==ConnectState.connectActive) waitingForConnect = true;
            if(connectionState!=ConnectionState.CONNECTED) {
                String message = channelName + " connection state " + connectionState.name();
                message(message,MessageType.error);
                channelConnectStatus = statusCreate.createStatus(StatusType.ERROR,message,null);
                connectState = ConnectState.notConnected;
            } else {
                connectState = ConnectState.connected;
                channelConnectStatus = statusCreate.getStatusOK();
            }   
            if(waitingForConnect) waitForConnect.signal();
        } finally {
            lock.unlock();
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
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        String mess = channelName + " " + message;
        pvaClient.message(mess, messageType);
    }
    /**
     * Destroy the pvAccess connection and all cached gets and puts.
     */
    public void destroy()
    {
        if(isDestroyed) return;
        isDestroyed = true;
        if(channel!=null) channel.destroy();
        pvaClientGetCache.destroy();
        pvaClientPutCache.destroy();
    }

    /**
     * Get the name of the channel to which pvaClientChannel is connected.
     * @return The channel name.
     */
    public String getChannelName()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        return channelName;
    }

    /**
     * Get the Channel to which pvaClientChannel is connected.
     * @return The channel.
     */
    public Channel getChannel()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        return channel;
    }

    /**
     * Connect to the channel.
     * This calls issueConnect and waitConnect.
     * 
     * @param timeout The time to wait for connecting to the channel.
     * @throws RuntimeException if connection fails.
     */
    public void connect(double timeout)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        issueConnect();
        Status status = waitConnect(timeout);
        if(status.isOK()) return;
        String mess = "channel "
                + getChannelName()
                + " PvaClientChannel::connect "
                + status.getMessage();
        throw new RuntimeException(mess);
    }

    /**
     * Issue a connect request and return immediately.
     */
    public void issueConnect()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        if(connectState!=ConnectState.connectIdle) {
            throw new RuntimeException("pvaClientChannel already connected");
        }
        channelConnectStatus = statusCreate.createStatus(
                StatusType.ERROR,
                getChannelName() + " createChannel failed",null);
        connectState = ConnectState.connectActive;
        ChannelProvider provider = ChannelProviderRegistryFactory
                .getChannelProviderRegistry().getProvider(providerName);
        if(provider==null) {
            String mess = getChannelName() + " provider " + providerName + " not registered";
            throw new RuntimeException(mess);
        }
        channel = provider.createChannel(channelName, this, ChannelProvider.PRIORITY_DEFAULT);
        if(channel==null) {
            String mess = getChannelName() + " channelCreate failed ";
            throw new RuntimeException(mess);
        }
    }

    /**
     * Wait until the connection completes or for timeout.
     * If failure getStatus can be called to get reason.
     * @param timeout The time in second to wait.
     * @return status of connect request.
     */
    public Status waitConnect(double timeout)
    {
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        lock.lock();
        try {
            if(connectState==ConnectState.connected) return channelConnectStatus;
            try {
                long nano = (long)(timeout*1e9);
                waitForConnect.awaitNanos(nano);
            } catch(InterruptedException e) {
                Status status = statusCreate.createStatus(StatusType.ERROR,e.getMessage(), e.fillInStackTrace());
                return status;
            }
            return channelConnectStatus;
        } finally {
            lock.unlock();
        }
    }


    /**
     * Calls the next method with subField = "";
     * @return The interface.
     */
    public PvaClientField createField()
    {
        return createField("");
    }

    /**
     * Create an EasyField for the specified subField.
     * @param subField The syntax for subField is defined in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientField createField(String subField)
    {
        throw new RuntimeException("pvaClientChannel::createField not implemented");
    }

    /**
     * Calls the next method with request = "";
     * @return The interface.
     */
    public PvaClientProcess createProcess()
    {
        return createProcess("");
    }

    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientProcess createProcess(String request)
    {
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String mess = "channel " + getChannelName() 
                    + " PvaClientChannel::createProcess invalid pvRequest: " + createRequest.getMessage();
            throw new RuntimeException(mess);
        }
        return createProcess(pvRequest);
    }

    /**
     * Creates a PvaClientProcess. 
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientProcess createProcess(PVStructure pvRequest)
    {
        return PvaClientProcess.create(pvaClient,channel,pvRequest);
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @return The interface.
     */
    public PvaClientGet get()
    {
        return get("value,alarm,timeStamp");
    }

    /**
     * Get a cached PvaClientGet or create and connect to a new PvaClientGet.
     * Then call it's get method.
     * If connection or get can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientGet get(String request)
    {
        PvaClientGet pvaClientGet = pvaClientGetCache.getGet(request);
        if(pvaClientGet!=null) return pvaClientGet;
        pvaClientGet = createGet(request);
        pvaClientGet.connect();
        pvaClientGetCache.addGet(request, pvaClientGet);
        return pvaClientGet;
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @return The interface.
     */
    public PvaClientGet createGet()
    {
        return createGet("value,alarm,timeStamp");
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientGet createGet(String request)
    {
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String mess = "channel " + getChannelName() 
                    + " PvaClientChannel::createGet invalid pvRequest: " + createRequest.getMessage();
            throw new RuntimeException(mess);
        }
        return createGet(pvRequest);
    }
    /**
     * Creates an EasyGet.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientGet createGet(PVStructure pvRequest)
    {
        return PvaClientGet.create(pvaClient,channel,pvRequest);
    }

    /**
     * Call the next method with request =  "field(value)" 
     * @return The interface.
     */
    public PvaClientPut put()
    {
        return put("value");
    }

    /**
     *  Get a cached PvaClientPut or create and connect to a new PvaClientPut.
     *  Then call it's get method.
     *  If connection can not be made an exception is thrown.
     *  @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientPut put(String request)
    {
        PvaClientPut pvaClientPut = pvaClientPutCache.getPut(request);
        if(pvaClientPut!=null) return pvaClientPut;
        pvaClientPut = createPut(request);
        pvaClientPut.connect();
        pvaClientPut.get();
        pvaClientPutCache.addPut(request,pvaClientPut);
        return pvaClientPut;
    }

    /**
     *  Call the next method with request = "field(value)" 
     * @return The interface.
     */
    public PvaClientPut createPut()
    {
        return createPut("value");
    }

    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientPut createPut(String request)
    {
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String mess = "channel " + getChannelName() 
                    + " PvaClientChannel::createPut invalid pvRequest: "
                    + createRequest.getMessage();
            throw new RuntimeException(mess);
        }
        return createPut(pvRequest);
    } 

    /**
     * Create an EasyPut.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientPut createPut(PVStructure pvRequest)
    {
        return PvaClientPut.create(pvaClient,channel,pvRequest);
    }

    /**
     *  Call the next method with request = "record[process=true]putField(argument)getField(result)".
     * @return The interface.
     */
    public PvaClientPutGet createPutGet()
    {
        return createPutGet("putField(argument)getField(result)");
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientPutGet createPutGet(String request)
    {
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String mess = "channel " + getChannelName() 
                    + " PvaClientChannel::createPutGet invalid pvRequest: "
                    + createRequest.getMessage();
            throw new RuntimeException(mess);
        }
        return createPutGet(pvRequest);
    }

    /**
     * Create an EasyPutGet.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientPutGet createPutGet(PVStructure pvRequest)
    {
        return PvaClientPutGet.create(pvaClient,channel,pvRequest);
    }

    /**
     * Call the next method with request = "field(value)";
     * @return The interface.
     */
    public  PvaClientArray createArray()
    {
        return createArray("");
    }

    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientArray createArray(String request)
    {
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String mess = "channel " + getChannelName() 
                    + " PvaClientChannel::createArray invalid pvRequest: "
                    + createRequest.getMessage();
            throw new RuntimeException(mess);
        }
        return createArray(pvRequest);
    }
    /**
     * Create a pvaClientArray.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientArray createArray(PVStructure pvRequest)
    {
        String message = channelName + " createArray is not implemented";
        throw new RuntimeException(message);
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @return The interface.
     */
    public PvaClientMonitor monitor()
    {
        return monitor("value,alarm,timeStamp");
    }

    /**
     * Get a cached PvaClientMonitor or create and connect to a new PvaClientMonitor.
     * Then call it's start method.
     * If connection can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientMonitor monitor(String request)
    {
        PvaClientMonitor clientMonitor = createMonitor(request);
        clientMonitor.connect();
        clientMonitor.start();
        return clientMonitor;
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @param pvaClientMonitorRequester The client callback.
     * @return The interface.
     */
    public PvaClientMonitor monitor(
            PvaClientMonitorRequester pvaClientMonitorRequester)
    {
        return monitor("value,alarm,timeStamp",pvaClientMonitorRequester);
    }

    /**
     * Get a cached PvaClientMonitor or create and connect to a new PvaClientMonitor.
     * Then call it's start method.
     * If connection can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @param pvaClientMonitorRequester The client callback.
     * @return The interface.
     */
    public PvaClientMonitor monitor(
            String request,
            PvaClientMonitorRequester pvaClientMonitorRequester)
    {
        PvaClientMonitor clientMonitor = createMonitor(request);
        clientMonitor.connect();
        clientMonitor.setRequester(pvaClientMonitorRequester);
        clientMonitor.start();
        return clientMonitor;
    }

    /**
     * Call the next method with request = "field(value.alarm,timeStamp)" 
     * @return The interface.
     */
    public PvaClientMonitor createMonitor()
    {
        return createMonitor("value,alarm,timeStamp");
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientMonitor createMonitor(String request)
    {
        PVStructure pvRequest = createRequest.createRequest(request);
        if(pvRequest==null) {
            String mess = "channel " + getChannelName() 
                    + " PvaClientChannel::createMonitor invalid pvRequest: "
                    + createRequest.getMessage();
            throw new RuntimeException(mess);
        }
        return createMonitor(pvRequest);
    }
    /**
     * Create an EasyMonitor.
     * @param pvRequest  The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientMonitor createMonitor(PVStructure pvRequest)
    {
        return PvaClientMonitor.create(pvaClient, channel, pvRequest);
    }

    /** Show the list of cached gets and puts.
     */
    public String showCache()
    {
        return pvaClientGetCache.toString() + pvaClientPutCache.toString();
    }
     /** Get the number of cached gets and puts.
     */
    public int cacheSize()
    {
          return pvaClientGetCache.cacheSize() + pvaClientPutCache.cacheSize();
    }
   
}
