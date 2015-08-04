/* PvaClientMultiMonitorDouble.java */
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

import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.Status;


/**
 * @author mrk
 * This provides a monitor to multiple channels where each channel has a numeric scalar value field.
 */
public class PvaClientMultiMonitorDouble
{
    /**
     * Factory method that creates a PvaClientMultiMonitorDouble.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @return The interface.
     */
    static public PvaClientMultiMonitorDouble create(
         PvaClientMultiChannel pvaClientMultiChannel,
         PvaClientChannel[] pvaClientChannelArray)
    {
          return new PvaClientMultiMonitorDouble(pvaClientMultiChannel,pvaClientChannelArray);
    }
   
    /** Destroy the pvAccess connection.
     */
    public void destroy()
    {
        if(isDestroyed) return;
        isDestroyed = true;
        pvaClientChannelArray = null;
    }
     /**
     * Create a channel monitor for each channel.
     */
    public void connect()
    {
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        String request = "value";
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   pvaClientMonitor[i] = pvaClientChannelArray[i].createMonitor(request);
                   pvaClientMonitor[i].issueConnect();
             }
        }
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   Status status = pvaClientMonitor[i].waitConnect();
                   if(status.isOK()) continue;
                   String message = "channel " + pvaClientChannelArray[i].getChannelName();
                   message += " PvaChannelMonitor::waitConnect " + status.getMessage();
                   throw new RuntimeException(message);
             }
        }
        isMonitorConnected = true;
    }
    /**
     * poll each channel.
     * If any has new data it is used to update the double[].
     * @return (false,true) if (no, at least one) value was updated.
     */
    public boolean poll()
    {
        if(!isMonitorConnected){
            connect();
            try
            {
                Thread.sleep(100);
            } catch (Throwable th) {
                th.printStackTrace();
            }
        }
       boolean result = false;
       boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
       for(int i=0; i<nchannel; ++i)
       {
            if(isConnected[i]) {
                 if(pvaClientMonitor[i].poll()) {
                      doubleValue[i] = pvaClientMonitor[i].getData().getDouble();
                      pvaClientMonitor[i].releaseEvent();
                      result = true;
                 }
            }
       }
       return result;
    }
    /**
     * Wait until poll returns true.
     * @param waitForEvent The time to keep trying.
     * A thread sleep of .1 seconds occurs between each call to poll.
     * @return (false,true) if (timeOut, poll returned true).
     */
    public boolean waitEvent(double waitForEvent)
    {
        if(poll()) return true;
        TimeStamp start = TimeStampFactory.create();
        start.getCurrentTime();
        TimeStamp now  = TimeStampFactory.create();
        while(true) {
            try
            {
                Thread.sleep(100);
            } catch (Throwable th) {
                th.printStackTrace();
            }
              if(poll()) return true;
              now.getCurrentTime();
              double diff = start.diff(now, start);
              if(diff>=waitForEvent) break;
        }
        return false;
    }
    /**
     * get the data.
     *  @return The double[] where each element is the value field of the corresponding channel.
     */
    public double[] get()
    {
        return doubleValue;
    }

    private PvaClientMultiMonitorDouble(
         PvaClientMultiChannel pvaClientMultiChannel,
         PvaClientChannel[] pvaClientChannelArray)
    {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        nchannel = pvaClientChannelArray.length;
        doubleValue = new double[nchannel];
        pvaClientMonitor = new PvaClientMonitor[nchannel];
        for(int i=0; i<nchannel; ++i) {
            {
                pvaClientMonitor[i] = null;
                doubleValue[i] = Double.NaN;
            }
        }  
    }
    
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private PvaClientChannel[] pvaClientChannelArray;
    private int nchannel;
    
    private double[] doubleValue;
    boolean isGetConnected = false;
    private PvaClientMonitor[] pvaClientMonitor;
    boolean isMonitorConnected;
    boolean isDestroyed = false;
    
};

