/* PvaClientNTMultiPut.java */
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

import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.FieldBuilder;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.Status;

/**
 *  This provides channelPut to multiple channels where the value field of each channel is presented as a union.
 */
public class PvaClientNTMultiPut
{
    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    private static final Convert convert = ConvertFactory.getConvert();
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final PvaClientChannel[] pvaClientChannelArray;
    private final int nchannel;
    private final ReentrantLock lock = new ReentrantLock();

    private PVUnion[] unionValue;
    private PVField[] value;
    private PvaClientPut[] pvaClientPut;
    private boolean isConnected = false;
    private boolean isDestroyed = false;

    /**
     * Factory method that creates a PvaClientNTMultiPut.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @return The interface.
     */
    public static PvaClientNTMultiPut create(
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[] pvaClientChannelArray)
    {
        return new PvaClientNTMultiPut(pvaClientMultiChannel,pvaClientChannelArray);
    }
    /** Destroy the pvAccess connection.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientNTMultiPut::destroy()");
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
     * Create a channelPut for each channel.
     */
    public void connect()
    {
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                pvaClientPut[i] = pvaClientChannelArray[i].createPut();
                pvaClientPut[i].issueConnect();
            }
        }
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                Status status = pvaClientPut[i].waitConnect();
                if(status.isOK()) continue;
                String message = "channel "
                        + pvaClientChannelArray[i].getChannelName()
                        + " PvaChannelPut::waitConnect "
                        + status.getMessage();
                throw new RuntimeException(message);
            }
        }
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                pvaClientPut[i].issueGet();
            }
        }
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                Status status = pvaClientPut[i].waitGet();
                if(status.isOK()) continue;
                String message = "channel "
                        + pvaClientChannelArray[i].getChannelName()
                        + " PvaChannelPut::waitGet "
                        + status.getMessage();
                throw new RuntimeException(message);
            }
        }
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                value[i] = pvaClientPut[i].getData().getValue();
                FieldBuilder fb = fieldCreate.createFieldBuilder();
                fb.add("value",value[i].getField());
                unionValue[i] = pvDataCreate.createPVUnion(fb.createUnion());
            }
        }
        this.isConnected = true;
    }
    /**
     * get the value field of each channel as a union.
     * @return A shared vector of union.
     */
    public PVUnion[] getValues()
    {
        if(!isConnected) connect();
        return unionValue;
    }
    /**
     * put the data to each channel.
'    */
    public void put()
    {
        if(!isConnected) connect();
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                convert.copy(unionValue[i].get(),value[i]);
                pvaClientPut[i].issuePut();
            }
            if(isConnected[i]) {
                Status status = pvaClientPut[i].waitPut();
                if(status.isOK())  continue;
                String message = "channel "
                        + pvaClientChannelArray[i].getChannelName()
                        + " PvaChannelPut::waitPut "
                        + status.getMessage();
                throw new RuntimeException(message);
            }
        }
    }

    private PvaClientNTMultiPut(
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[] pvaClientChannelArray)
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientNTMultiPut::PvaClientNTMultiPut()");
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        nchannel = pvaClientChannelArray.length;
        unionValue = new PVUnion[nchannel];
        value = new PVField[nchannel];
        pvaClientPut = new PvaClientPut[nchannel];
        for(int i=0; i<nchannel; ++i)
        {
            unionValue[i] = null;
            value[i] = null;
            pvaClientPut[i] = null;
        }
    }


};
