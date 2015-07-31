/* PvaClientMultiGetDouble.java */
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

import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;

/**
 * @author mrk
 *  This provides channelGet to multiple channels where each channel has a numeric scalar value field.
 */
public class PvaClientMultiGetDouble
{

    /**
     * Factory method that creates a PvaClientMultiGetDouble.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @return The interface.
     */
    static public PvaClientMultiGetDouble create(
         PvaClientMultiChannel pvaClientMultiChannel,
         PvaClientChannel[] pvaClientChannelArray)
    {
          return new PvaClientMultiGetDouble(pvaClientMultiChannel,pvaClientChannelArray);
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
     * Create a channelGet for each channel.
     */
    public void connect()
    {
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        String request = "value";
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   pvaClientGet[i] = pvaClientChannelArray[i].createGet(request);
                   pvaClientGet[i].issueConnect();
             }
        }
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   Status status = pvaClientGet[i].waitConnect();
                   if(status.isOK()) continue;
                   String message = "channel " + pvaClientChannelArray[i].getChannelName();
                   message += " PvaChannelGet::waitConnect " + status.getMessage();
                   throw new RuntimeException(message);
             }
        }
        isGetConnected = true;
    }
    /**
     * get the data.
     * @return The double[] where each element is the value field of the corresponding channel.
     */
    public double[] get()
    {
        if(!isGetConnected) connect();
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                   pvaClientGet[i].issueGet();
             }
        }
        for(int i=0; i<nchannel; ++i)
        {
             if(isConnected[i]) {
                 Status status = pvaClientGet[i].waitGet();
                 if(status.isOK()) continue;
                 String message = "channel " + pvaClientChannelArray[i].getChannelName();
                 message += " PvaChannelGet::waitGet " + status.getMessage();
                 throw new RuntimeException(message);
             }
        }
        
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i])
            {
                PVStructure pvStructure = pvaClientGet[i].getData().getPVStructure();
                doubleValue[i] = convert.toDouble(pvStructure.getSubField(PVScalar.class,"value"));
            } else {
                doubleValue[i] = Double.NaN;
            }
        }
        return doubleValue;
    }

    
    private PvaClientMultiGetDouble(
         PvaClientMultiChannel pvaClientMultiChannel,
         PvaClientChannel[] pvaClientChannelArray)
    {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        nchannel = pvaClientChannelArray.length;
        doubleValue = new double[nchannel];
        pvaClientGet = new PvaClientGet[nchannel];
        for(int i=0; i<nchannel; ++i) {
            {
                pvaClientGet[i] = null;
                doubleValue[i] = Double.NaN;
            }
        }
    }
    private static final Convert convert = ConvertFactory.getConvert();
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private PvaClientChannel[] pvaClientChannelArray;
    private int nchannel;
    
    private double[] doubleValue;
    private PvaClientGet[] pvaClientGet;
    boolean isGetConnected = false;
    boolean isDestroyed = false;
};

