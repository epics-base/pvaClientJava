/* PvaClientNTMultiData.Java */
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

import org.epics.nt.NTMultiChannel;
import org.epics.nt.NTMultiChannelBuilder;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.property.Alarm;
import org.epics.pvdata.property.AlarmSeverity;
import org.epics.pvdata.property.AlarmStatus;
import org.epics.pvdata.property.PVTimeStamp;
import org.epics.pvdata.property.PVTimeStampFactory;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.BooleanArrayData;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.IntArrayData;
import org.epics.pvdata.pv.LongArrayData;
import org.epics.pvdata.pv.PVBooleanArray;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVIntArray;
import org.epics.pvdata.pv.PVLong;
import org.epics.pvdata.pv.PVLongArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.PVUnionArray;
import org.epics.pvdata.pv.StringArrayData;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.Union;
import org.epics.pvdata.pv.UnionArrayData;


/**
 *  This provides NTMultiChannel data for both PvaClientNTMultiGet and PvaClientNTMultiMonitor.
 */
public class PvaClientNTMultiData
{
    /**
     * Factory method that creates a PvaClientNTMultiData.
     * Normally only called by PvaClientNTMultiGet and PvaClientNTMultiMonitor.
     * @param u The union interface for the value field of each channel.
     * @param pvaClientMultiChannel The interface to PvaClientMultiChannel.
     * @param pvaClientChannelArray The PvaClientChannel array.
     * @param pvRequest The pvRequest for each channel.
     * @return The interface.
     */
    public static PvaClientNTMultiData create(
            Union  u,
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[]  pvaClientChannelArray,
            PVStructure pvRequest)
    {
        return new PvaClientNTMultiData(u,pvaClientMultiChannel,pvaClientChannelArray,pvRequest);
    }

    private PvaClientNTMultiData(
            Union  u,
            PvaClientMultiChannel pvaClientMultiChannel,
            PvaClientChannel[]  pvaClientChannelArray,
            PVStructure pvRequest)
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientNTMultiData::PvaClientNTMultiData()");
        this.pvaClientMultiChannel = pvaClientMultiChannel;
        this.pvaClientChannelArray = pvaClientChannelArray;
        nchannel = pvaClientChannelArray.length;
        PVField pvValue = pvRequest.getSubField("field.value");
        if(pvValue==null) {
            throw new RuntimeException("pvRequest did not specify value");
        }
        topPVStructure = new PVStructure[nchannel];
        unionValue = new PVUnion[nchannel];
        for(int i=0; i< nchannel; ++i) {
            topPVStructure[i] = null;
            unionValue[i] = pvDataCreate.createPVUnion(u);
        }
        NTMultiChannelBuilder builder = NTMultiChannel.createBuilder();
        builder.value(u).addIsConnected();
        if(pvRequest.getSubField("field.alarm")!=null)
        {
            gotAlarm = true;
            builder.addAlarm();
            builder.addSeverity();
            builder.addStatus();
            builder.addMessage();
            severity = new int[nchannel];
            status = new int[nchannel];
            message = new String[nchannel];

        }
        if(pvRequest.getSubField("field.timeStamp")!=null) {
            gotTimeStamp = true;
            builder.addTimeStamp();
            builder.addSecondsPastEpoch();
            builder.addNanoseconds();
            builder.addUserTag();
            secondsPastEpoch = new long[nchannel];
            nanoseconds = new int[nchannel];
            userTag = new int[nchannel];
        }
        ntMultiChannelStructure = builder.createStructure();
    }


    void setStructure(Structure  structure,int index)
    {
        Field field = structure.getField("value");
        if(field==null) {
            String message = "channel "
                    + pvaClientChannelArray[index].getChannel().getChannelName()
                    + " does not have top level value field";
            throw new RuntimeException(message);
        }
    }

    void setPVStructure(PVStructure pvStructure,int index)
    {
        topPVStructure[index] = pvStructure;
    }

    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    private static final Convert convert = ConvertFactory.getConvert();
    private final PvaClientMultiChannel pvaClientMultiChannel;
    private final PvaClientChannel[] pvaClientChannelArray;
    private final int nchannel;
    private final ReentrantLock lock = new ReentrantLock();

    private PVStructure[] topPVStructure;
    private boolean gotAlarm = false;
    private boolean gotTimeStamp = false;
    private boolean isDestroyed = false;

    private Structure ntMultiChannelStructure;
    private PVUnion[] unionValue;
    private UnionArrayData unionArrayData = new UnionArrayData();
    private IntArrayData intArrayData = new IntArrayData();
    private LongArrayData longArrayData = new LongArrayData();
    private StringArrayData stringArrayData = new StringArrayData();
    private BooleanArrayData booleanArrayData = new BooleanArrayData();
    private int[] severity;
    private int[] status;
    private String[] message;
    private long[] secondsPastEpoch;
    private int[] nanoseconds;
    private int[] userTag;
    private Alarm alarm = new Alarm();
    private TimeStamp timeStamp = TimeStampFactory.create();
    private PVTimeStamp pvTimeStamp = PVTimeStampFactory.create();

    /** Destroy the pvAccess connection.
     */
    public void destroy()
    {
        if(PvaClient.getDebug()) System.out.println("PvaClientNTMultiData::destroy()");
        lock.lock();
        try {
            if(isDestroyed) return;
            isDestroyed = true;
        } finally {
            lock.unlock();
        }
        for(int i=0; i<nchannel; ++i) {
            pvaClientChannelArray[i] = null;
            topPVStructure[i] = null;
        }
    }

    /**
     * Get the number of channels.
     * @return The number of channels.
     */
    public int getNumber()
    {
        return nchannel;
    }

    /**
     * Set the timeStamp base for computing deltaTimes. 
     */
    public void startDeltaTime()
    {

        for(int i=0; i<nchannel; ++i)
        {
            topPVStructure[i] = null;
            if(gotAlarm)
            {
                alarm.setSeverity(AlarmSeverity.NONE);
                alarm.setStatus(AlarmStatus.NONE);
                alarm.setMessage("");
                severity[i] = AlarmSeverity.INVALID.ordinal();
                status[i] = AlarmStatus.UNDEFINED.ordinal();
                message[i] = "not connected";
            }
            if(gotTimeStamp)
            {
                timeStamp.getCurrentTime();
                secondsPastEpoch[i] = 0;
                nanoseconds[i] = 0;
                userTag[i] = 0;
            }
        }
    }

    /**
     * Update NTMultiChannel fields.
     */
    public void endDeltaTime()
    {
        for(int i=0; i<nchannel; ++i)
        {
            PVStructure pvst = topPVStructure[i];
            if(pvst==null) {
                unionValue[i].set(null);
            } else {
                unionValue[i].set(pvst.getSubField("value"));
                if(gotAlarm)
                {
                    severity[i] = pvst.getSubField(PVInt.class,"alarm.severity").get();
                    status[i] = pvst.getSubField(PVInt.class,"alarm.status").get();
                    message[i] = pvst.getSubField(PVString.class,"alarm.message").get();
                }
                if(gotTimeStamp)
                {
                    secondsPastEpoch[i] = pvst.getSubField(PVLong.class,"timeStamp.secondsPastEpoch").get();
                    nanoseconds[i] = pvst.getSubField(PVInt.class,"timeStamp.nanoseconds").get();
                    userTag[i] = pvst.getSubField(PVInt.class,"timeStamp.userTag").get();
                }
            }
        }

    }
    /**
     * Get the time when the last get was made.
     * @return The timeStamp.
     */
    public TimeStamp getTimeStamp()
    {
        pvTimeStamp.get(timeStamp);
        return timeStamp;
    }
    /**
     * Get the  NTMultiChannel.
     * @return The value.
     */
    public NTMultiChannel getNTMultiChannel()
    {
        PVStructure pvStructure = pvDataCreate.createPVStructure(ntMultiChannelStructure);
        NTMultiChannel ntMultiChannel = NTMultiChannel.wrap(pvStructure);
        convert.fromStringArray(ntMultiChannel.getChannelName(),0,nchannel,pvaClientMultiChannel.getChannelNames(),0);
        PVUnion[] val = new PVUnion[nchannel];
        for(int i=0; i<nchannel; ++i) val[i] = unionValue[i];
        PVUnionArray array = ntMultiChannel.getValue();
        array.setLength(nchannel);
        array.get(0, nchannel, unionArrayData);
        for(int i=0; i<nchannel; ++i) unionArrayData.data[i] = val[i];
        boolean[] connected = pvaClientMultiChannel.getIsConnected();
        boolean[] isConnected = new boolean[nchannel];
        for(int i=0; i<nchannel; ++i) isConnected[i] = connected[i];
        PVBooleanArray pvBooleanArray = ntMultiChannel.getIsConnected();
        pvBooleanArray.setLength(nchannel);
        pvBooleanArray.get(0, nchannel, booleanArrayData);
        for(int i=0; i<nchannel; ++i) booleanArrayData.data[i] = isConnected[i];
        if(gotAlarm)
        {
            int[] sev = new int[nchannel];
            for(int i=0; i<nchannel; ++i) sev[i] = severity[i];
            PVIntArray pvIntArray = ntMultiChannel.getSeverity();
            pvIntArray.setLength(nchannel);
            pvIntArray.get(0, nchannel, intArrayData);
            for(int i=0; i<nchannel; ++i) intArrayData.data[i] = sev[i];
            int[] sta = new int[nchannel];
            for(int i=0; i<nchannel; ++i) sta[i] = status[i];
            pvIntArray = ntMultiChannel.getStatus();
            pvIntArray.setLength(nchannel);
            pvIntArray.get(0, nchannel, intArrayData);
            for(int i=0; i<nchannel; ++i) intArrayData.data[i] = sev[i];
            String[] mes = new String[nchannel];
            for(int i=0; i<nchannel; ++i) mes[i] = message[i];
            PVStringArray pvStringArray = ntMultiChannel.getMessage();
            pvStringArray.setLength(nchannel);
            pvStringArray.get(0, nchannel, stringArrayData);
            for(int i=0; i<nchannel; ++i) stringArrayData.data[i] = mes[i];
        }
        if(gotTimeStamp)
        {
            long[] sec = new long[nchannel];
            for(int i=0; i<nchannel; ++i) sec[i] = secondsPastEpoch[i];
            PVLongArray pvLongArray = ntMultiChannel.getSecondsPastEpoch();
            pvLongArray.setLength(nchannel);
            pvLongArray.get(0, nchannel, longArrayData);
            for(int i=0; i<nchannel; ++i) longArrayData.data[i] = sec[i];
            int[]  nano = new int[nchannel];
            for(int i=0; i<nchannel; ++i) nano[i] = nanoseconds[i];
            PVIntArray pvIntArray = ntMultiChannel.getNanoseconds();
            pvIntArray.setLength(nchannel);
            pvIntArray.get(0, nchannel, intArrayData);
            for(int i=0; i<nchannel; ++i) intArrayData.data[i] = nano[i];
            int[] tag = new int[nchannel];
            for(int i=0; i<nchannel; ++i) tag[i] = userTag[i];
            pvIntArray = ntMultiChannel.getUserTag();
            pvIntArray.setLength(nchannel);
            pvIntArray.get(0, nchannel, intArrayData);
            for(int i=0; i<nchannel; ++i) intArrayData.data[i] = tag[i];
        }
        return ntMultiChannel;
    }
};

