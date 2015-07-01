/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
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
    static public PvaClientMultiChannel create()
    {
    }

    public PvaClientMultiChannel(
            PvaClient pvaClient,
            String[] channelNames,
            String providerName,
            Union union)
    {
        this.easyPVA = easyPVA;
        this.channelName = channelNames;
        this.providerName = providerName;
        this.union = union;
        numChannel = channelNames.length;
        channelRequester = new ChannelRequesterPvt[numChannel];
        channel = new Channel[numChannel];
        connectRequested = new boolean[numChannel];
        isConnected = new boolean[numChannel];
        channelStatus = new Status[numChannel];
        connectionState = new ConnectionState[numChannel];
        for(int i=0; i<numChannel; ++i) {
            channelRequester[i] = new ChannelRequesterPvt(this,i);
            channel[i] = null;
            connectRequested[i] = false;
            isConnected[i] = false;
            channelStatus[i] = statusCreate.getStatusOK();
            connectionState[i] = ConnectionState.NEVER_CONNECTED;
        }
    }

    private final PvaClient easyPVA;
    private final String[] channelName;
    private final String providerName;
    private final Union union;
    private final int numChannel;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition waitForConnect = lock.newCondition();

    private ChannelRequesterPvt[] channelRequester = null;
    private volatile int numConnected = 0;
    private volatile int numNewConnectRequest = 0;
    private volatile Channel[] channel = null;
    private volatile boolean[] connectRequested = null;
    private volatile boolean[] isConnected = null;

    private volatile boolean isDestroyed = false;
    private volatile Status[] channelStatus = null;
    private volatile Status status = statusCreate.getStatusOK();
    private volatile ConnectionState[] connectionState = null;

    private boolean checkConnected() {
        if(numConnected==0) connect(3.0);
        if(numConnected==channelName.length) return true;
        return false;
    }
    /**
     * Get the requester name.
     * @return The name.
     */
     public String getRequesterName()
    {
        return easyPVA.getRequesterName();
    } 
    /**
     * new message.
     * @param message The message string.
     * @param messageType The message type.
     */
     public void message(String message, MessageType messageType)
     {
         if(isDestroyed) return;
         String mess = channelName + " " + message;
         easyPVA.message(mess, messageType);
     }
     /**
      * Get the channelNames.
      * @return the names.
      */
     String[] getChannelNames()
     {
         return channelName
     }
     /**
      * Destroy all resources.
      */
     void destroy()
     {
         synchronized (this) {
             if(isDestroyed) return;
             isDestroyed = true;
         }
         for(int i=0; i< channelName.length; ++i) {
             if(channel[i]!=null) channel[i].destroy();
         }
     }
     /**
      * Calls issueConnect and then waitConnect.
      * @param timeout timeOut for waitConnect.
      * @return (false,true) if (not connected, connected)
      */
     boolean connect(double timeout)
     {
         issueConnect();
         return waitConnect(timeout);
     }
     /**
      * Calls issueConnect and then waitConnect.
      * @param timeout timeOut for waitConnect
      * @param minConnect minConnect for waitConnect
      * @return (false,true) means (did not, did) connect.
      */
     boolean connect(double timeout,int minConnect)
     {
         issueConnect();
         return waitConnect(timeout,minConnect);
     }
     /**
      * Connect to all channels.
      */
     void issueConnect()
     {
         if(isDestroyed) return;
         if(numNewConnectRequest!=0) {
             throw new RuntimeException("multiChannel issueConnect called multiple times ");
         }
         ChannelProvider channelProvider = ChannelProviderRegistryFactory.getChannelProviderRegistry().getProvider(providerName);
         numNewConnectRequest = 0;
         for(int i=0; i<channelName.length; ++i) {
             if(connectRequested[i]) continue;
             numNewConnectRequest++;
             connectRequested[i] = true;
             isConnected[i] = false;
             channel[i] = channelProvider.createChannel(
                     channelName[i],
                     channelRequester[i],
                     ChannelProvider.PRIORITY_DEFAULT);   
         }
     }
     /**
      * Wait until all channels are connected or until timeout.
      * @param timeout The time to wait for connections.
      * When a timeout occurs a new time out will start if any channels connected since the last timeout.
      * @return (false,true) if all channels (did not, did) connect.
      */
     boolean waitConnect(double timeout)
     {
         return waitConnect(timeout,numChannel);
     }
     /**
      * Wait until minConnect channels are connected or until timeout and no more channels connect.
      * @param timeout The time to wait for connections.
      * @param minConnect The minimum number of channels that must connect.
      * When a timeout occurs a new time out will start if any channels connected since the last timeout.
      * @return (false,true) of all channels (did not, did) connect.
      */
     boolean waitConnect(double timeout,int minConnect)
     {
         if(isDestroyed) return false;
         int numNowConected = 0;
         while(true) {
             try {
                 lock.lock();
                 try {
                     if(numNewConnectRequest==0) break;
                     long nano = (long)(timeout*1e9);
                     numNowConected = numConnected;
                     if(numConnected<numChannel) waitForConnect.awaitNanos(nano);
                 } catch(InterruptedException e) {
                     String message = "channel "
                             + " InterruptedException " + e.getMessage();
                     throw new RuntimeException(message);
                 }
             } finally {
                 lock.unlock();
             }
             if(numConnected==numChannel) break;
             if(numNowConected==numConnected) break;  // no new so quit 
             if(numNowConected<minConnect)  continue;
             break;
         }
         if(numConnected<minConnect) {
             String message = " channels " + numChannel + " but only " + numNowConected + " are connected";
             throw new RuntimeException(message);
         }
         return true;
     }
     /**
      * Are all channels connected?
      * @return if all are connected.
      */
     boolean allConnected()
     {
         if(numConnected==numChannel) return true;
         return false;
     }
     /**
      * Get the connection state of each channel.
      * @return The state of each channel.
      */
     boolean[] isConnected()
     {
         return isConnected;
     }
     /**
      * Create an EasyMultiData.
      * @param pvRequest The pvRequest for each channel.
      * @param union The union for each channel.
      * @return The interface.
      */
     PvaClientMultiData createEasyMultiData(PVStructure pvRequest,Union union)
     {
         return PvaClientMultiDataImpl.create(this,channel,pvRequest,union);
     }
     /**
      * create a multiChannelGet that presents data as a NTMultiChannel.
      * calls the next method with request = "field(value,alarm,timeStamp)"
      * @return The interface.
      */
     PvaClientMultiGet createGet()
     {
         return createGet(false);
     }
     /**
      * create a multiChannelGet.
      * calls the next method after creating a pvRequest structure.
      * @param request A request string valid for creatRequest.
      * @return The interface.
      */
     PvaClientMultiGet createGet(String request)
     {
         return createGet(false,request);
     }
     /**
      * create a multiChannelGet.
      * @param pvRequest The pvRequest for each channel.
      * @return The interface.
      */
     PvaClientMultiGet createGet(PVStructure pvRequest)
     {
         return createGet(false,pvRequest);
     }
     /**
      * create a multiChannelGet.
      * @param doubleOnly true if data presented as a double[].
      * @return The interface.
      */
     PvaClientMultiGet createGet(boolean doubleOnly)
     {
         String request = doubleOnly ? "value" : "value,alarm,timeStamp";
         return createGet(doubleOnly,request);
     }
     /**
      * create a multiChannelGet.
      * calls the next method with request = "field(value)"
      * @param doubleOnly true if data presented as a double[].
      * @param request  A request string valid for creatRequest.
      * @return EasyMultiGet or null if invalid request.
      */
     PvaClientMultiGet createGet(boolean doubleOnly,String request)
     {
         PVStructure pvStructure = createRequest(request);
         if(pvStructure==null) return null;
         return createGet(doubleOnly,pvStructure);
     }
     /**
      * create a multiChannelGet.
      * @param doubleOnly true if data presented as a double[].
      * @param pvRequest The pvRequest for each channel.
      * @return  EasyMultiGet or null if invalid request.
      */
     PvaClientMultiGet createGet(boolean doubleOnly,PVStructure pvRequest)
     {
         if(!checkConnected()) return null;
         Union union = this.union;
         if(doubleOnly) {
             Field[] field = new Field[1];
             String[] name = new String[1];
             name[0] = "double";
             field[0] = fieldCreate.createScalar(ScalarType.pvDouble);
             union = fieldCreate.createUnion(name, field);
         }
         return  PvaClientMultiGetImpl.create(this,channel,pvRequest,union);
     }
     /**
      * create a multiChannelPut.
      * @return The interface.
      */
     PvaClientMultiPut createPut()
     {
         return createPut(false);
     }
     /**
      * create a multiChannelPut.
      * @param doubleOnly true if data must be presented as a double[].
      * @return EasyMultiPut or null if invalid request.
      */
     PvaClientMultiPut createPut(boolean doubleOnly)
     {
         if(!checkConnected()) return null;
         return PvaClientMultiPutImpl.create(this,channel,doubleOnly);
     }
     /**
      * Call the next method with request =  "field(value,alarm,timeStamp)" 
      * @return The interface.
      */
     PvaClientMultiMonitor createMonitor()
     {{
         return createMonitor(false);
     }
     /**
      * First call createRequest as implemented by pvDataJava and then calls the next method.
      * @param request The request as described in package org.epics.pvdata.copy
      * @return The interface.
      */
     PvaClientMultiMonitor createMonitor(String request)

     return createMonitor(false,request);
     }
     /**
      * Creates an EasyMultiMonitor.
      * The pvRequest is used to create the monitor for each channel.
      * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
      * @return The interface.
      */
     PvaClientMultiMonitor createMonitor(PVStructure pvRequest)
     {
         return createMonitor(false,pvRequest);
     }
     /**
      * Call the next method with request =  "field(value,alarm,timeStamp)" 
      * @param doubleOnly true if data must be presented as a double[].
      * @return The interface.
      */
     PvaClientMultiMonitor createMonitor(boolean doubleOnly)
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
     PvaClientMultiMonitor createMonitor(boolean doubleOnly,String request)
     {
         PVStructure pvRequest = createRequest(request);
         if(pvRequest==null) return null;
         return createMonitor(doubleOnly,pvRequest);
     }
     /**
      * Creates an EasyMultiMonitor.
      * The pvRequest is used to create the monitor for each channel.
      * @param doubleOnly true if data must be presented as a double[].
      * @param pvRequest The syntax of pvRequest is described in package org.epics.pvdata.copy.
      * @return The interface.
      */
     PvaClientMultiMonitor createMonitor(boolean doubleOnly,PVStructure pvRequest)
     {
         if(!checkConnected()) return null;
         Union union = this.union;
         if(doubleOnly) {
             Field[] field = new Field[1];
             String[] name = new String[1];
             name[0] = "double";
             field[0] = fieldCreate.createScalar(ScalarType.pvDouble);
             union = fieldCreate.createUnion(name, field);
         }
         return PvaClientMultiMonitorImpl.create(this, channel, pvRequest,union);
     }
}
