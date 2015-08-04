/* PvaClientMultiPutDouble.java */
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

import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;


/**
 * @author mrk
 * This provides channelPut to multiple channels where each channel has a numeric scalar value field.
 */
public class PvaClientMultiPutDouble
{
    /**
     * Factory method that creates a PvaClientMultiPutDouble.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @return The interface.
     */
    static public PvaClientMultiPutDouble create(
         PvaClientMultiChannel pvaClientMultiChannel,
         PvaClientChannel[] pvaClientChannelArray)
    {
          return new PvaClientMultiPutDouble(pvaClientMultiChannel,pvaClientChannelArray);
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
     * Create a channelPut for each channel.
     */
    public void connect()
    {
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        String request = "value";
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   pvaClientPut[i] = pvaClientChannelArray[i].createPut(request);
                   pvaClientPut[i].issueConnect();
             }
        }
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   Status status = pvaClientPut[i].waitConnect();
                   if(status.isOK()) continue;
                   String message = "channel " + pvaClientChannelArray[i].getChannelName();
                   message += " PvaChannelPut::waitConnect " + status.getMessage();
                   throw new RuntimeException(message);
             }
        }
        isPutConnected = true;
    }
    /** put data to each channel as a double
     * @param data The array of data for each channel.
     */
    public void put(double[] data)
    {
        if(!isPutConnected) connect();
        if(data.length!=nchannel) {
             throw new RuntimeException("data has wrong size");
        }
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   PVStructure pvTop = pvaClientPut[i].getData().getPVStructure();
                   PVDouble pvValue = pvTop.getSubField(PVDouble.class,"value");
                   pvValue.put(data[i]);
                   pvaClientPut[i].issuePut();
             }
             if(isConnected[i]) {
                  Status status = pvaClientPut[i].waitPut();
                  if(status.isOK())  continue;
                  String message = "channel " + pvaClientChannelArray[i].getChannelName();
                  message += " PvaChannelPut::waitConnect " + status.getMessage();
                  throw new RuntimeException(message);
             }
        }
    }

    private PvaClientMultiPutDouble(
         PvaClientMultiChannel pvaClientMultiChannel,
         PvaClientChannel[] pvaClientChannelArray)
    {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        nchannel = pvaClientChannelArray.length;
        doubleValue = new double[nchannel];
        pvaClientPut = new PvaClientPut[nchannel];
        for(int i=0; i<nchannel; ++i) {
            {
                pvaClientPut[i] = null;
                doubleValue[i] = Double.NaN;
            }
        }
    }

    private final PvaClientMultiChannel pvaClientMultiChannel;
    private PvaClientChannel[] pvaClientChannelArray;
    private int nchannel;
    
    private double[] doubleValue;
    private PvaClientPut[] pvaClientPut;
    boolean isPutConnected = false;
    boolean isDestroyed = false;
    
    
};
