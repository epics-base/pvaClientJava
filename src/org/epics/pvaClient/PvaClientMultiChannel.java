/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvaccess.client.Channel;
import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Union;

/**
 * An  interface to get/put/monitor data from/to multiple channels.
 * @author mrk
 *
 */
public class PvaClientMultiChannel {
    /**
     * Create a PvaClientMultiChannel.
     * This has providerName = "pva" and union = variantUnion.
     * @param pvaClient The pvaClient.
     * @param channelNames The array of channel names.
     * @return The interface.
     */
    static public PvaClientMultiChannel create(
            PvaClient pvaClient,
            String[] channelNames)
    {
        return PvaClientMultiChannel.create(pvaClient,channelNames,"pva",variantUnion);
    }
    /**
     * Create a PvaClientMultiChannel.
     * This has union = variantUnion.
     * @param pvaClient The pvaClient.
     * @param channelNames The array of channel names.
     * @param providerName The provider name.
     * @return The interface.
     */
    static public PvaClientMultiChannel create(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName)
    {
        return new PvaClientMultiChannel(pvaClient,channelNames,providerName,variantUnion);
    }
    /**
     * Create a PvaClientMultiChannel.
     * This has providerName = "pva".
     * @param pvaClient The pvaClient.
     * @param channelNames The array of channel names.
     * @param union The union in case data is an NTNDArray.
     * @return The interface.
     */
    static public PvaClientMultiChannel create(
            PvaClient pvaClient,
            String[] channelNames,
            Union union)
    {
        return PvaClientMultiChannel.create(pvaClient,channelNames,"pva",union);
    }
    /**
     * Create a PvaClientMultiChannel.
     * @param pvaClient The pvaClient.
     * @param channelNames The array of channel names.
     * @param providerName The provider name.
     * @param union The union in case data is an NTNDArray.
     * @return The interface.
     */
    static public PvaClientMultiChannel create(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName,
            Union union)
    {
        return new PvaClientMultiChannel(pvaClient,channelNames,providerName,union);
    }

    private PvaClientMultiChannel(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName,
            Union union)
    {
        this.pvaClient = pvaClient;
        this.channelName = channelNames;
        this.providerName = providerName;
        this.union = union;
        numChannel = channelNames.length;
    }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    private static final Union variantUnion = FieldFactory.getFieldCreate().createVariantUnion();
    private final PvaClient pvaClient;
    private final String[] channelName;
    private final String providerName;
    private final Union union;
    private final int numChannel;

    private volatile int numConnected = 0;
    private volatile PvaClientChannel[] pvaClientChannelArray = null;
    private volatile Channel[] channel = null;
    private volatile boolean[] isConnected = null;

    private volatile boolean isDestroyed = false;
    
    private void checkConnected() {
        if(numConnected==0) connect(3.0,0);
    }

    private static PVStructure createRequest(String request)
    {
        CreateRequest factory = CreateRequest.create();
        PVStructure pvStructure = factory.createRequest(request);
        if (pvStructure == null) 
            throw new IllegalArgumentException("invalid pvRequest: " + factory.getMessage());
        else
            return pvStructure;
    }
    
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
             channel[i] = null;
         }
     }
     
      /**
      * Get the channelNames.
      * @return the names.
      */
     public String[] getChannelNames()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
         return channelName;
     }
     /** Connect to the channels.
     * This calls issueConnect and waitConnect.
     * An exception is thrown if connect fails.
     * @param timeout The time to wait for connecting to the channel.
     * @param maxNotConnected Maximum number of channels that do not connect.
     */
     public void connect(double timeout,int maxNotConnected)
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
         channel = new Channel[numChannel];
         isConnected = new boolean[numChannel];
         for(int i=0; i<numChannel; ++i)
         {
             isConnected[i] = false;
             channel[i] = null;
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
                 channel[i] = pvaClientChannelArray[i].getChannel();
                 continue;
             }
             if(returnStatus.isOK()) returnStatus = status;
             ++numBad;
             if(numBad>maxNotConnected) break;
         }
         if(numBad>maxNotConnected) {
             String message = "pvaClientMultiChannel::connect number not connected " + numBad;
             message += " but maxNotConnected " + maxNotConnected;
             throw new RuntimeException(message);
         }
     }
     /** Are all channels connected?
     * @return if all are connected.
     */
     public boolean allConnected()
     {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
         if(pvaClientChannelArray==null) throw new RuntimeException("pvaClientMultiChannel not connected");
         return (numConnected==numChannel) ? true : false;
     }
    /** Has a connection state change occurred?
     * @return (true, false) if (at least one, no) channel has changed state.
     */
    public boolean connectionChange()
    {
         if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
         if(pvaClientChannelArray==null) throw new RuntimeException("pvaClientMultiChannel not connected");
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
    public boolean[] getIsConnected()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        if(pvaClientChannelArray==null)throw new RuntimeException("pvaClientMultiChannel not connected");
        for(int i=0; i<numChannel; ++i) isConnected[i] = false;
        PvaClientChannel[] channels = pvaClientChannelArray;
        for(int i=0; i<numChannel; ++i) {
            PvaClientChannel pvaClientChannel = channels[i];
            Channel channel = pvaClientChannel.getChannel();
            Channel.ConnectionState stateNow = channel.getConnectionState();
            isConnected[i] = (stateNow==Channel.ConnectionState.CONNECTED) ? true : false;
        }
        return isConnected;
    }
    /** Get the pvaClientChannel array.
     * @return The array.
     */
    public PvaClientChannel[] getPvaClientChannelArray()
    {
        if(isDestroyed) throw new RuntimeException("pvaClientMultiChannel was destroyed");
        if(pvaClientChannelArray==null) throw new RuntimeException("pvaClientMultiChannel not connected");
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
     * Create an PvaClientMultiData.
     * @param pvRequest The pvRequest for each channel.
     * @param union The union for each channel.
     * @return The interface.
     */
    public PvaClientMultiData createPvaClientMultiData(PVStructure pvRequest,Union union)
    {
        return PvaClientMultiData.create(this, channel, pvRequest, union);
    }
    /**
     * create a multiChannelGet that presents data as a NTMultiChannel.
     * calls the next method with request = "field(value,alarm,timeStamp)"
     * @return The interface.
     */
    public PvaClientMultiGet createGet()
    {
        return createGet(false);
    }
    /**
     * create a multiChannelGet.
     * calls the next method after creating a pvRequest structure.
     * @param request A request string valid for creatRequest.
     * @return The interface.
     */
    public PvaClientMultiGet createGet(String request)
    {
        return createGet(false,request);
    }
    /**
     * create a multiChannelGet.
     * @param pvRequest The pvRequest for each channel.
     * @return The interface.
     */
    public PvaClientMultiGet createGet(PVStructure pvRequest)
    {
        return createGet(false,pvRequest);
    }
    /**
     * create a multiChannelGet.
     * @param doubleOnly true if data presented as a double[].
     * @return The interface.
     */
    public PvaClientMultiGet createGet(boolean doubleOnly)
    {
        String request = doubleOnly ? "value" : "value,alarm,timeStamp";
        return createGet(doubleOnly,request);
    }
    /**
     * create a multiChannelGet.
     * calls the next method with request = "field(value)"
     * @param doubleOnly true if data presented as a double[].
     * @param request  A request string valid for creatRequest.
     * @return PvaClientMultiGet or null if invalid request.
     */
    public PvaClientMultiGet createGet(boolean doubleOnly,String request)
    {
        PVStructure pvStructure = createRequest(request);
        if(pvStructure==null) return null;
        return createGet(doubleOnly,pvStructure);
    }
    /**
     * create a multiChannelGet.
     * @param doubleOnly true if data presented as a double[].
     * @param pvRequest The pvRequest for each channel.
     * @return  PvaClientMultiGet or null if invalid request.
     */
    public PvaClientMultiGet createGet(boolean doubleOnly,PVStructure pvRequest)
    {
        checkConnected();
        Union union = this.union;
        if(doubleOnly) {
            Field[] field = new Field[1];
            String[] name = new String[1];
            name[0] = "double";
            field[0] = fieldCreate.createScalar(ScalarType.pvDouble);
            union = fieldCreate.createUnion(name, field);
        }
        return  PvaClientMultiGet.create(this,channel,pvRequest,union);
    }
    /**
     * create a multiChannelPut.
     * @return The interface.
     */
    public PvaClientMultiPut createPut()
    {
        return createPut(false);
    }
    /**
     * create a multiChannelPut.
     * @param doubleOnly true if data must be presented as a double[].
     * @return PvaClientMultiPut or null if invalid request.
     */
    public PvaClientMultiPut createPut(boolean doubleOnly)
    {
        checkConnected();
        return PvaClientMultiPut.create(this,channel,doubleOnly);
    }
    /**
     * Call the next method with request =  "field(value,alarm,timeStamp)" 
     * @return The interface.
     */
    public PvaClientMultiMonitor createMonitor()
    {
        return createMonitor(false);
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientMultiMonitor createMonitor(String request)
    {
        return createMonitor(false,request);
    }
    /**
     * Creates an PvaClientMultiMonitor.
     * The pvRequest is used to create the monitor for each channel.
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientMultiMonitor createMonitor(PVStructure pvRequest)
    {
        return createMonitor(false,pvRequest);
    }
    /**
     * Call the next method with request =  "field(value,alarm,timeStamp)" 
     * @param doubleOnly true if data must be presented as a double[].
     * @return The interface.
     */
    public PvaClientMultiMonitor createMonitor(boolean doubleOnly)
    {
        String request = doubleOnly ? "value" : "value,alarm,timeStamp";
        return createMonitor(doubleOnly,request);
    }
    /**
     * First call createRequest as implemented by pvDataJava and then calls the next method.
     * @param doubleOnly true if data must be presented as a double[].
     * @param request The request as described in package org.epics.pvdata.copy
     * @return The interface.
     */
    public PvaClientMultiMonitor createMonitor(boolean doubleOnly,String request)
    {
        PVStructure pvRequest = createRequest(request);
        if(pvRequest==null) return null;
        return createMonitor(doubleOnly,pvRequest);
    }
    /**
     * Creates an PvaClientMultiMonitor.
     * The pvRequest is used to create the monitor for each channel.
     * @param doubleOnly true if data must be presented as a double[].
     * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
     * @return The interface.
     */
    public PvaClientMultiMonitor createMonitor(boolean doubleOnly,PVStructure pvRequest)
    {
        checkConnected();
        Union union = this.union;
        if(doubleOnly) {
            Field[] field = new Field[1];
            String[] name = new String[1];
            name[0] = "double";
            field[0] = fieldCreate.createScalar(ScalarType.pvDouble);
            union = fieldCreate.createUnion(name, field);
        }
        return PvaClientMultiMonitor.create(this, channel, pvRequest,union);
    }
}
