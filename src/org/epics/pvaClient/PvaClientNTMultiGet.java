/* pvaClientNTMultiGet.cpp */
/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
/**
 * @author mrk
 * @date 2015.08
 */

package org.epics.pvaClient;

import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Union;

/**
 *  This provides channelGet to multiple channels where the value field of each channel is presented as a union.
 */
public class PvaClientNTMultiGet
{
    private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final PvaClientChannel[] pvaClientChannelArray;
    private final int nchannel;
    private final ReentrantLock lock = new ReentrantLock();

    private final PVStructure pvRequest;
    private PvaClientNTMultiData pvaClientNTMultiData;
    private PvaClientGet[] pvaClientGet;
    private boolean isConnected = false;
    private boolean isDestroyed = false;

    /**
     * Factory method that creates a PvaClientNTMultiGet.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @param pvRequest The pvRequest for each channel.
     * @return The interface.
     */
    public static PvaClientNTMultiGet create(
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[] pvaClientChannelArray,
            PVStructure   pvRequest)
    {
        Union u = fieldCreate.createVariantUnion();
        return new PvaClientNTMultiGet(u,pvaClientMultiChannel,pvaClientChannelArray,pvRequest);
    }
    /** Destroy the pvAccess connection.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientNTMultiGet::destroy()");
        lock.lock();
        try {
            if(isDestroyed) return;
            isDestroyed = true;
        } finally {
            lock.unlock();
        }
        for(int i=0; i<nchannel; ++i) pvaClientChannelArray[i] = null;
    }
    /**
     * Create a channelGet for each channel.
     */
    public void connect()
    {
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        String request = "value";
        if(pvRequest.getSubField("field.alarm")!=null) request += ",alarm";
        if(pvRequest.getSubField("field.timeStamp")!=null) request += ",timeStamp";
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
                String message = "channel "
                        + pvaClientChannelArray[i].getChannelName()
                        + " PvaChannelGet::waitConnect "
                        + status.getMessage();
                throw new RuntimeException(message);
            }
        }
        this.isConnected = true;
    }
    /**
     * get data for each channel.
     */
    public void get()
    {
        if(!isConnected) connect();
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
                String message = "channel "
                        + pvaClientChannelArray[i].getChannelName()
                        + " PvaChannelGet::waitGet "
                        + status.getMessage();
                throw new RuntimeException(message);
            }
        }
        pvaClientNTMultiData.startDeltaTime();
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                pvaClientNTMultiData.setPVStructure(pvaClientGet[i].getData().getPVStructure(),i);
            }
        }
        pvaClientNTMultiData.endDeltaTime();
    }
    /**
     * get the data.
     * @return the pvaClientNTMultiData.
     */
    public PvaClientNTMultiData getData()
    {
        return pvaClientNTMultiData;
    }

    private PvaClientNTMultiGet(
            Union  u,
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[] pvaClientChannelArray,
            PVStructure pvRequest)
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientNTMultiGet::PvaClientNTMultiGet()");
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        this.pvRequest = pvRequest;
        nchannel = pvaClientChannelArray.length;
        pvaClientNTMultiData = PvaClientNTMultiData.create(
                u,
                pvaClientMultiChannel,
                pvaClientChannelArray,
                pvRequest);
        pvaClientGet = new PvaClientGet[nchannel];
        for(int i=0; i<nchannel; ++i)
        {
            pvaClientGet[i] = null;
        }
    }
};

