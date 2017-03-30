
/*pvaClientChannel.java*/
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
 * @since 2015.06
 *
 */
public class PvaClientChannel implements ChannelRequester,Requester{
    
    /**
     * Create an instance of PvaClientChannel.
     * @param pvaClient The single instance of pvaClient.
     * @param channelName The channel name.
     * @param providerName The provider name.
     * @return The new instance.
     */
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
            if(pvaClientGetMap.get(request)!=null) {
                throw new RuntimeException("pvaClientGetCache::addGet pvaClientGet already cached");
            }
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
            if(pvaClientPutMap.get(request)!=null) {
                throw new RuntimeException("pvaClientPutCache::addPut pvaClientPut already cached");
            }
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

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();

    private volatile Channel channel = null;
    private volatile PvaClientChannelStateChangeRequester stateChangeRequester = null;

    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRequester#channelCreated(org.epics.pvdata.pv.Status, org.epics.pvaccess.client.Channel)
     */
    @Override
    public void channelCreated(Status status, Channel channel) {
        if(PvaClient.getDebug()) {
            System.out.println(
                    "PvaClientChannel::channelCreated"
                            + " channel " + getChannelName()
                            + " connectState " + connectState
                            + " isConnected " + channel.isConnected()
                            + " status.isOK " + (status.isOK() ? "true" : "false")
                    );
        }
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        this.channel = channel;
        if(connectState!=ConnectState.connectActive) {
            String message = "PvaClientChannel::channelCreated"
                    + " channel " + getChannelName()
                    + " why was this called when connectState!=ConnectState.connectActive";
            throw new RuntimeException(message);
        }
        if(channel.isConnected()) {
            lock.lock();
            try {
                connectState = ConnectState.connected;
                waitForConnect.signal();
            } finally {
                lock.unlock();
            }
        }
    }
    /* (non-Javadoc)
     * @see org.epics.pvaccess.client.ChannelRequester#channelStateChange(org.epics.pvaccess.client.Channel, org.epics.pvaccess.client.Channel.ConnectionState)
     */
    @Override
    public void channelStateChange(Channel channel,ConnectionState connectionState) {
        if(PvaClient.getDebug()) {
            System.out.println(
                    "PvaClientChannel::channelStateChange"
                            + " channel " + getChannelName()
                            + " isConnected " + (connectionState==ConnectionState.CONNECTED ? "true" : "false")
                    );
        }
        if(stateChangeRequester!=null) {
            boolean value = (connectionState==ConnectionState.CONNECTED ? true : false);
            stateChangeRequester.channelStateChange(this, value);
        }
        boolean waitingForConnect = false;
        if(isDestroyed) return;
        if(connectState==ConnectState.connectActive) waitingForConnect = true;
        if(connectionState!=ConnectionState.CONNECTED) {
            String message = channelName + " connection state " + connectionState.name();
            message(message,MessageType.error);
            connectState = ConnectState.notConnected;
        } else {
            connectState = ConnectState.connected;
        }

        if(waitingForConnect) {
            lock.lock();
            try {
                waitForConnect.signal();
            }  finally {
                lock.unlock();
            }
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
        String mess = channelName + " " + message;
        pvaClient.message(mess, messageType);
    }
    /**
     * Destroy the pvAccess connection and all cached gets and puts.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientChannel::destroy() "
                    + "channel " + getChannelName());
        }
        synchronized (this) {
            if(isDestroyed) return;
            isDestroyed = true;
        }
        if(channel!=null) {
            channel=null;
        }
        if(PvaClient.getDebug()) showCache();
        pvaClientGetCache.destroy();
        pvaClientPutCache.destroy();
    }

    /**
     * Get the name of the channel to which pvaClientChannel is connected.
     * @return The channel name.
     */
    public String getChannelName()
    {
        return channelName;
    }

    /**
     * Get the Channel to which pvaClientChannel is connected.
     * @return The channel.
     */
    public Channel getChannel()
    {
        return channel;
    }
    /**
     * Set a user callback for change of state.
     * @param stateChangeRequester The user supplied requester.
     */
    public void setStateChangeRequester(
            PvaClientChannelStateChangeRequester stateChangeRequester)
    {
        this.stateChangeRequester = stateChangeRequester;
        stateChangeRequester.channelStateChange(this,channel.isConnected());
    }
    /**
     * Clear user callback for change of state.
     */
    public void clearRequester()
    {
        stateChangeRequester = null; 
    }
    /**
     * connect with timeout set to 5 seconds.
     */
    public void connect()
    {
        connect(5.0);
    }
    /**
     * Connect to the channel.
     * This calls issueConnect and waitConnect.
     * 
     * @param timeout The time to wait for connecting to the channel.
     * A value of 0.0 means forever.
     * @throws RuntimeException if connection fails.
     */
    public void connect(double timeout)
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientChannel::connect() "
                    + "channel " + getChannelName());
        }
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        issueConnect();
        Status status = waitConnect(timeout);
        if(status.isOK()) return;
        if(PvaClient.getDebug()) System.out.println("PvaClientChannel::waitConnect() failed");
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
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientChannel::issueConnect() "
                    + "channel " + getChannelName());
        }

        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        if(connectState==ConnectState.connected) return;
        if(connectState!=ConnectState.connectIdle) {
            throw new RuntimeException("pvaClientChannel already connected");
        }
        connectState = ConnectState.connectActive;	

        ChannelProvider provider = ChannelProviderRegistryFactory
                .getChannelProviderRegistry().getProvider(providerName);
        if(provider==null) {
            String mess = getChannelName() + " provider " + providerName + " not registered";
            throw new RuntimeException(mess);
        }
        if(PvaClient.getDebug()) System.out.println("PvaClientChannel::issueConnect calling provider->createChannel");
        channel = provider.createChannel(channelName, this, ChannelProvider.PRIORITY_DEFAULT);
        if(channel==null) {
            String mess = getChannelName() + " channelCreate failed ";
            throw new RuntimeException(mess);
        }
    }
    /**
     * Wait until the connection completes or for timeout.
     * If failure getStatus can be called to get reason
     * This calls the next method with a time out of 5 seconds.
     * @return status of connect request.
     */
    public Status waitConnect()
    {
        return waitConnect(5.0);
    }
    /**
     * Wait until the connection completes or for timeout.
     * If failure getStatus can be called to get reason.
     * @param timeout The time in second to wait.
     *  A value of 0 means forever.
     * @return status of connect request.
     */
    public Status waitConnect(double timeout)
    {
        if(PvaClient.getDebug()) {
            System.out.println("PvaClientChannel::waitConnect() "
                    + "channel " + getChannelName());
        }
        if(isDestroyed) throw new RuntimeException("pvaClientChannel was destroyed");
        if(channel.isConnected()) return statusCreate.getStatusOK();
        lock.lock();
        try {
            try {
                if(timeout>0.0) {
                    long nano = (long)(timeout*1e9);
                    waitForConnect.awaitNanos(nano);
                } else {
                    waitForConnect.await();
                }
            } catch(InterruptedException e) {
                Status status = statusCreate.createStatus(StatusType.ERROR,e.getMessage(), e.fillInStackTrace());
                return status;
            }
        } finally {
            lock.unlock();
        }
        if(channel.isConnected()) return statusCreate.getStatusOK();
        return statusCreate.createStatus(StatusType.ERROR,"channel not connected",null);
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
     * Create an PvaClientField for the specified subField.
     * @param subField The syntax for subField is defined in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientField createField(String subField)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        throw new RuntimeException("pvaClientChannel::createField not implemented");
    }

    /**
     * Calls the next method with request = "";
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientProcess createProcess()
    {
        return createProcess("");
    }

    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * @throws RuntimeException if create fails.
     */
    public PvaClientProcess createProcess(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        return PvaClientProcess.create(pvaClient,channel,pvRequest);
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientGet get()
    {
        return get("field(value,alarm,timeStamp)");
    }

    /**
     * Get a cached PvaClientGet or create and connect to a new PvaClientGet.
     * Then call it's get method.
     * If connection or get can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * @throws RuntimeException if create fails.
     */
    public PvaClientGet createGet()
    {
        return createGet("field(value,alarm,timeStamp)");
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * Creates a pvaClientGet.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientGet createGet(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        return PvaClientGet.create(pvaClient,channel,pvRequest);
    }

    /**
     * Call the next method with request =  "field(value)" 
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientPut put()
    {
        return put("field(value)");
    }

    /**
     *  Get a cached PvaClientPut or create and connect to a new PvaClientPut.
     *  Then call it's get method.
     *  If connection can not be made an exception is thrown.
     *  @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * @throws RuntimeException if create fails.
     */
    public PvaClientPut createPut()
    {
        return createPut("field(value)");
    }

    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * Create an PvaClientPut.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientPut createPut(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        return PvaClientPut.create(pvaClient,channel,pvRequest);
    }

    /**
     *  Call the next method with request = "record[process=true]putField(argument)getField(result)".
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientPutGet createPutGet()
    {
        return createPutGet("putField(argument)getField(result)");
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * Create an PvaClientPutGet.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientPutGet createPutGet(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        return PvaClientPutGet.create(pvaClient,channel,pvRequest);
    }

    /**
     * Call the next method with request = "field(value)";
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public  PvaClientArray createArray()
    {
        return createArray("field(value)");
    }

    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
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
     * @throws RuntimeException if create fails.
     */
    public PvaClientArray createArray(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        String message = channelName + " createArray is not implemented";
        throw new RuntimeException(message);
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor monitor()
    {
        return monitor("field(value,alarm,timeStamp)");
    }

    /**
     * Get a cached PvaClientMonitor or create and connect to a new PvaClientMonitor.
     * Then call it's start method.
     * If connection can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor monitor(String request)
    {
        return monitor(request,null);
    }

    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @param pvaClientMonitorRequester The client callback.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor monitor(
            PvaClientMonitorRequester pvaClientMonitorRequester)
    {
        return monitor("field(value,alarm,timeStamp)",pvaClientMonitorRequester);
    }

    /**
     * Create and connect to a new PvaClientMonitor.
     * Then call it's start method.
     * If connection can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @param pvaClientMonitorRequester The client callback.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor monitor(
            String request,
            PvaClientMonitorRequester pvaClientMonitorRequester)
    {
        return monitor(request,pvaClientMonitorRequester,null);
    }
    /**
     * Call the next method with request =  "value,alarm,timeStamp" 
     * @param pvaClientMonitorRequester The client callback.
     * @param pvaClientListenerRequester The client callback.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor monitor(
            PvaClientMonitorRequester pvaClientMonitorRequester,
            PvaClientUnlistenRequester pvaClientListenerRequester)
    {
        return monitor("field(value,alarm,timeStamp)",pvaClientMonitorRequester,pvaClientListenerRequester);
    }

    /**
     * Create and connect to a new PvaClientMonitor.
     * Then call it's start method.
     * If connection can not be made an exception is thrown.
     * @param request The request as described in package org.epics.pvdata.copy
     * @param pvaClientMonitorRequester The client callback.
     *  @param pvaClientListenerRequester The client callback.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor monitor(
            String request,
            PvaClientMonitorRequester pvaClientMonitorRequester,
            PvaClientUnlistenRequester pvaClientListenerRequester)
    {
        PvaClientMonitor clientMonitor = createMonitor(request);
        clientMonitor.connect();
        clientMonitor.setRequester(pvaClientMonitorRequester);
        clientMonitor.setUnlistenRequester(pvaClientListenerRequester);
        clientMonitor.start();
        return clientMonitor;
    }

    /**
     * Call the next method with request = "field(value.alarm,timeStamp)" 
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor createMonitor()
    {
        return createMonitor("field(value,alarm,timeStamp)");
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     * @throws RuntimeException if create fails.
     * @return The monitor.
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
     * Create an PvaClientMonitor.
     * @param pvRequest  The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     * @throws RuntimeException if create fails.
     */
    public PvaClientMonitor createMonitor(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        return PvaClientMonitor.create(pvaClient, channel, pvRequest);
    }
    /** Issue a channelRPC request
     * @param pvRequest  The pvRequest that is passed to createRPC.
     * @param pvArgument  The argument for a request.
     * @return The result.
     */
    public PVStructure rpc(
            PVStructure pvRequest,
            PVStructure pvArgument)
    {
        PvaClientRPC rpc = createRPC(pvRequest);
        return rpc.request(pvArgument);
    }
    /** Issue a channelRPC request
     * @param pvArgument  The argument for the request.
     * @return The result.
     */
    public PVStructure rpc(
            PVStructure pvArgument)
    {
        PvaClientRPC rpc = createRPC();
        return rpc.request(pvArgument);
    }
    /** Create a PvaClientRPC.
     * @return The interface.
     */
    public PvaClientRPC createRPC()
    {
        if(connectState!=ConnectState.connected) connect(5.0);
        return PvaClientRPC.create(pvaClient,channel);   
    }
    /** Create a PvaClientRPC.
     * @param pvRequest  The pvRequest that must have the same interface
     *  as a pvArgument that is passed to an rpc request.
     * @return The interface.
     */
    public PvaClientRPC createRPC(PVStructure pvRequest)
    {
        if(connectState!=ConnectState.connected) connect(5.0);
       
        return PvaClientRPC.create(pvaClient,channel,pvRequest);   
    }

    /** Show the list of cached gets and puts.
     * @return A String showing the current cache.
     */
    public String showCache()
    {
        return pvaClientGetCache.toString() + pvaClientPutCache.toString();
    }
    /** Get the number of cached gets and puts.
     * @return The size.
     */
    public int cacheSize()
    {
        return pvaClientGetCache.cacheSize() + pvaClientPutCache.cacheSize();
    }

}
