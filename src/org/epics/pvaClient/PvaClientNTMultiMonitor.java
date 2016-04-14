/* pvaClientNTMultiMonitor.cpp */
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
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Union;
/**
 *  This provides channel monitor to multiple channels where the value field of each channel is presented as a union.
 */
public class PvaClientNTMultiMonitor
{
    private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final PvaClientChannel[] pvaClientChannelArray;
    private final PVStructure pvRequest;
    private final int nchannel;
    private final ReentrantLock lock = new ReentrantLock();

    private PvaClientNTMultiData pvaClientNTMultiData;
    private PvaClientMonitor[] pvaClientMonitor;
    private boolean isConnected = false;
    private boolean isDestroyed = false;
    private TimeStamp start = TimeStampFactory.create();
    private TimeStamp now = TimeStampFactory.create();

    /**
     * Factory method that creates a PvaClientNTMultiMonitor.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @param pvRequest The pvRequest for each channel.
     * @return The interface.
     */
    public static PvaClientNTMultiMonitor create(
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[] pvaClientChannelArray,
            PVStructure  pvRequest)
    {
        Union u = fieldCreate.createVariantUnion();
        return new PvaClientNTMultiMonitor(u,pvaClientMultiChannel,pvaClientChannelArray,pvRequest);
    }

    private PvaClientNTMultiMonitor(
            Union  u,
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[] pvaClientChannelArray,
            PVStructure pvRequest)
    {
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        this.pvRequest = pvRequest;
        nchannel = pvaClientChannelArray.length;
        pvaClientNTMultiData = PvaClientNTMultiData.create(
                u,
                pvaClientMultiChannel,
                pvaClientChannelArray,
                pvRequest);
        pvaClientMonitor = new PvaClientMonitor[nchannel];
        for(int i=0; i<nchannel; ++i)
        {
            pvaClientMonitor[i] = null;
        }
    }
    /** Destroy the pvAccess connection.
     */
    public void destroy()
    {
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
     * Create a channel monitor for each channel.
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
                pvaClientMonitor[i] = pvaClientChannelArray[i].createMonitor(request);
                pvaClientMonitor[i].issueConnect();
            }
        }
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                Status status = pvaClientMonitor[i].waitConnect();
                if(status.isOK()) continue;
                String message = "channel "
                        + pvaClientChannelArray[i].getChannelName()
                        + " PvaChannelMonitor::waitConnect "
                        + status.getMessage();
                throw new RuntimeException(message);
            }
        }
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) pvaClientMonitor[i].start();
        }
        this.isConnected = true;
    }
    /**
     * poll each channel.
     * If any has new data it is used to update the double[].
     * @return (false,true) if (no, at least one) value was updated.
     */
    public boolean poll()
    {
        if(!isConnected) connect();
        boolean result = false;
        boolean[] isConnected = pvaClientMultiChannel.getIsConnected();
        pvaClientNTMultiData.startDeltaTime();
        for(int i=0; i<nchannel; ++i)
        {
            if(isConnected[i]) {
                if(pvaClientMonitor[i].poll()) {
                    pvaClientNTMultiData.setPVStructure(
                            pvaClientMonitor[i].getData().getPVStructure(),i);
                    pvaClientMonitor[i].releaseEvent();
                    result = true;
                }
            }
        }
        if(result) pvaClientNTMultiData.endDeltaTime();
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
        start.getCurrentTime();
        while(true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
            if(poll()) return true;
            now.getCurrentTime();
            double diff = now.diff(now, start);
            if(diff>=waitForEvent) break;
        }
        return false;
    }
    /**
     * get the data.
     * @return the pvaClientNTMultiData.
     */
    public PvaClientNTMultiData getData()
    {
        return pvaClientNTMultiData;
    }
};

