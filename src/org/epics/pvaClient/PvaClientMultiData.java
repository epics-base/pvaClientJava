/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvaccess.client.Channel;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.factory.StandardFieldFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.property.PVTimeStamp;
import org.epics.pvdata.property.PVTimeStampFactory;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVIntArray;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUnionArray;
import org.epics.pvdata.pv.Scalar;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.StandardField;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;
import org.epics.pvdata.pv.StatusCreate;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.Type;
import org.epics.pvdata.pv.Union;
import org.epics.pvdata.pv.UnionArrayData;


/**
 * An easy way to get data from multiple channels.
 * @author mrk
 *
 */
public class  PvaClientMultiData {
    /**
     * Factory for creating a new EasyPVStructure.
     * @return The interface.
     */
    static PvaClientMultiData create(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
    {
        PvaClientMultiData multiData = new PvaClientMultiData(
                easyMultiChannel,channel,pvRequest,union);
        if(!multiData.init()) return null;
        return multiData;
    }

    PvaClientMultiData(
            PvaClientMultiChannel easyMultiChannel,
            Channel[] channel,
            PVStructure pvRequest,
            Union union)
            {
        this.easyMultiChannel = easyMultiChannel;
        this.channel = channel;
        this.pvRequest = pvRequest;
        this.union = union;
        this.nchannel = channel.length;
            }

    private static final StatusCreate statusCreate = StatusFactory.getStatusCreate();
    private static final Convert convert = ConvertFactory.getConvert();
    private static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    private static final StandardField standardField =
            StandardFieldFactory.getStandardField();
    private final PvaClientMultiChannel easyMultiChannel;
    private final Channel[] channel;
    private final PVStructure pvRequest;
    private final Union union;
    private final int nchannel;

    private boolean doubleOnly = false;

    private volatile PVStructure[] topPVStructure = null;
    private volatile int offsetToSeverity = -1;
    private volatile int[] alarmSeverity = null;
    private volatile int[] alarmStatus = null;
    private volatile String[] alarmMessage = null;
    private volatile PVIntArray pvSeverity = null;
    private volatile PVIntArray pvStatus = null;
    private volatile PVStringArray pvMessage = null;
    private volatile int offsetToDeltaTime = -1;
    private volatile double[] deltaTime= null;
    private volatile PVDoubleArray pvDeltaTime = null;
    private volatile double[] doubleValue = null;
    private volatile PVStructure pvTop = null;
    private volatile PVDoubleArray pvDoubleArray = null;
    private volatile PVUnionArray pvUnionArray = null;
    private volatile UnionArrayData unionArrayData = null;
    private volatile PVStructure pvTimeStampStructure = null;
    private volatile Status status = statusCreate.getStatusOK();




    private final TimeStamp deltaBase = TimeStampFactory.create();
    private final PVTimeStamp pvTimeStamp = PVTimeStampFactory.create();
    private final TimeStamp timeStamp = TimeStampFactory.create();

    private boolean init()
    {
        PVField pvValue = pvRequest.getSubField("field.value");
        if(pvValue==null ) {
            Status status = statusCreate.createStatus(StatusType.ERROR,"pvRequest did not specify value",null);
            setStatus(status);
            return false;
        }

        if(!union.isVariant()) {
            Field[] field = union.getFields();
            if(field.length==1){
                if(field[0].getType()==Type.scalar) {
                    Scalar scalar = (Scalar)field[0];
                    if(scalar.getScalarType()==ScalarType.pvDouble) doubleOnly = true;
                }
            }
        }
        topPVStructure = new PVStructure[nchannel];
        int nsub = 3; // value,pvName,timeStamp
        if(doubleOnly) {
            Field[] field = new Field[nsub];
            String[] fieldName = new String[nsub];
            fieldName[0] = "value";
            field[0] = fieldCreate.createScalarArray(ScalarType.pvDouble);
            fieldName[1] = "channelName";
            field[1] = fieldCreate.createScalarArray(ScalarType.pvString);
            fieldName[2] = "timeStamp";
            field[2] = standardField.timeStamp();
            doubleValue = new double[nchannel];
            pvTop = pvDataCreate.createPVStructure(fieldCreate.createStructure(fieldName, field));
            pvDoubleArray = pvTop.getSubField(PVDoubleArray.class, "value");
            PVStringArray pvChannelName = pvTop.getSubField(PVStringArray.class,"channelName");
            pvChannelName.put(0, nchannel,easyMultiChannel.getChannelNames(), 0);
            pvTimeStampStructure = pvTop.getSubField(PVStructure.class,"timeStamp");
            return true;
        }
        PVField pvAlarm = pvRequest.getSubField("field.alarm");
        PVField pvTimeStamp = pvRequest.getSubField("field.timeStamp");
        if(pvAlarm!=null) {
            offsetToSeverity = nsub;
            alarmSeverity = new int[nchannel];
            alarmStatus = new int[nchannel];
            alarmMessage = new String[nchannel];
            nsub+=3;
        }
        if(pvTimeStamp!=null) {
            offsetToDeltaTime = nsub;
            deltaTime = new double[nchannel];
            nsub+=1;
        }
        Field[] field = new Field[nsub];
        String[] fieldName = new String[nsub];
        fieldName[0] = "value";
        field[0] = fieldCreate.createUnionArray(union);
        fieldName[1] = "channelName";
        field[1] = fieldCreate.createScalarArray(ScalarType.pvString);
        fieldName[2] = "timeStamp";
        field[2] = standardField.timeStamp();
        if(offsetToSeverity>=0) {
            fieldName[offsetToSeverity] = "severity";
            fieldName[offsetToSeverity+1] = "status";
            fieldName[offsetToSeverity+2] = "message";
            field[offsetToSeverity] = fieldCreate.createScalarArray(ScalarType.pvInt);
            field[offsetToSeverity+1] = fieldCreate.createScalarArray(ScalarType.pvInt);
            field[offsetToSeverity+2] = fieldCreate.createScalarArray(ScalarType.pvString);
        }
        if(offsetToDeltaTime>=0) {
            fieldName[offsetToDeltaTime] = "deltaTime";
            field[offsetToDeltaTime] = fieldCreate.createScalarArray(ScalarType.pvDouble);
        }
        pvTop = pvDataCreate.createPVStructure(fieldCreate.createStructure(fieldName, field));
        pvUnionArray = pvTop.getUnionArrayField("value");
        pvUnionArray.setLength(nchannel);
        unionArrayData = new UnionArrayData();
        pvUnionArray.get(0, nchannel, unionArrayData);
        for(int i=0; i<nchannel; i++) {
            unionArrayData.data[i] = pvDataCreate.createPVUnion(union);
        }
        PVStringArray pvChannelName = pvTop.getSubField(PVStringArray.class,"channelName");
        pvChannelName.put(0, nchannel,easyMultiChannel.getChannelNames(), 0);
        pvTimeStampStructure = pvTop.getSubField(PVStructure.class,"timeStamp");
        if(offsetToSeverity>=0) {
            pvSeverity = pvTop.getSubField(PVIntArray.class,"severity");
            pvStatus = pvTop.getSubField(PVIntArray.class,"status");
            pvMessage = pvTop.getSubField(PVStringArray.class,"message");
        }
        if(offsetToDeltaTime>=0) {
            pvDeltaTime = pvTop.getSubField(PVDoubleArray.class,"deltaTime");
        }
        return true;
    }



    /**
     * Set the introspection interface for the specified  channel.
     * @param topStructure The interface.
     * @param indChannel The index of the channel.
     */
    void setStructure(Structure topStructure,int indChannel)
    {
        Field field = structure.getField("value");
        if(field==null) {
            setStatus(statusCreate.createStatus(
                    StatusType.ERROR,"channel " + channel[index].getChannelName()
                    +" does not have top level value field",null));
        } else {
            boolean success= true;
            if(doubleOnly) {
                if(field.getType()!=Type.scalar) {
                    success = false;
                } else {
                    Scalar scalar = (Scalar)field;
                    if(!scalar.getScalarType().isNumeric()) success = false;
                }
                if(!success) {
                    setStatus(statusCreate.createStatus(
                            StatusType.ERROR,"channel value is not a numeric scalar",null));
                }
            } else {
                if(!union.isVariant()) {
                    Field[] fields = union.getFields();
                    int ind = -1;
                    for(int i=0; i<fields.length; ++i) {
                        if(fields[i].equals(field)) {
                            ind = i;
                            break;
                        }
                    }
                    if(ind==-1) success = false;
                }
                if(success) {
                    PVField value = pvDataCreate.createPVField(field);
                    unionArrayData.data[index].set(value);

                }
            }
        }
    }
    /**
     * Update the data for the specified channel.
     * @param topPVStructure The newest data for the channel.
     * @param bitset The bitSet showing which fields have changed value.
     * @param indChannel The index of the channel.
     */
    void setPVStructure(PVStructure topPVStructure,BitSet bitset,int indChannel)
    {
        topPVStructure[index] = pvStructure;
        if(doubleOnly) {
            doubleValue[index] = convert.toDouble((PVScalar)pvStructure.getSubField("value"));
        } else {
            topPVStructure[index] = pvStructure;
            if(offsetToSeverity>=0) {
                alarmSeverity[index] = pvStructure.getSubField(PVInt.class,"alarm.severity").get();
                alarmStatus[index] = pvStructure.getSubField(PVInt.class,"alarm.status").get();
                alarmMessage[index] = pvStructure.getSubField(PVString.class,"alarm.message").get();
            }
            if(offsetToDeltaTime>=0) {
                pvTimeStamp.attach(pvStructure.getSubField("timeStamp"));
                pvTimeStamp.get(timeStamp);
                deltaTime[index] = deltaBase.diff(timeStamp, deltaBase);
            }
            PVField from = pvStructure.getSubField("value");
            PVField to = unionArrayData.data[index].get();
            convert.copy(from, to);
        }
    }
    /**
     * Get the number of channels.
     * @return The number of channels.
     */
    int getNumber()
    {
        return number;
    }

    /**
     * Set the timeStamp base for computing deltaTimes. 
     */
    void startDeltaTime()
    {
        deltaBase.getCurrentTime();
        pvTimeStamp.attach(pvTop.getSubField("timeStamp"));
        pvTimeStamp.set(deltaBase);
    }
    /**
     * Update NTMultiChannel fields and/or doubleOnly fields.
     */
    void endDeltaTime()
    {
        if(doubleOnly) {
            pvDoubleArray.put(0, nchannel, doubleValue, 0);
        }
        if(offsetToSeverity>=0) {
            pvSeverity.put(0, nchannel, alarmSeverity, 0);
            pvStatus.put(0, nchannel, alarmStatus, 0);
            pvMessage.put(0, nchannel, alarmMessage, 0);
        }
        if(offsetToDeltaTime>=0) {
            pvDeltaTime.put(0, nchannel, deltaTime, 0);
        }
    }
    /**
     * Get the time when the last get was made.
     * @return The timeStamp.
     */
    TimeStamp getTimeStamp()
    {
        if(pvTimeStampStructure!=null) {
            pvTimeStamp.attach(pvTimeStampStructure);
            pvTimeStamp.get(timeStamp);
        }
        return timeStamp;
    }
    /**
     * Is value a double[] ?
     * @return The answer.
     */
    boolean doubleOnly()
    {
        return doubleOnly;
    }
    /**
     * Get the value field as a MTMultiChannel structure.
     * @return The value.
     * This is null if doubleOnly is true.
     */
    PVStructure getNTMultiChannel()
    {
        if(doubleOnly) return null;
        return pvTop;
    }

    /**
     * Get the top level structure if the value field is a double[]
     * @return The top level structure.
     * This is null if doubleOnly is false.
     */
    PVStructure getPVTop()
    {
        return pvTop;
    }
    /**
     * Return the value field.
     * @return The double[]
     * This is null if doubleOnly is false.
     */
    double[] getDoubleArray()
    {
        return doubleValue;
    }
    /**
     * Get the data from the value field.
     * @param offset The offset into the data of the value field.
     * @param data The place to copy the data.
     * @param length The number of elements to copy.
     * @return The number of elements copied.
     * This is 0 if doubleOnly is false.
     */
    int getDoubleArray(int offset, double[]data,int length)
    {
        int num = length;
        if(doubleValue.length-offset<length) num = doubleValue.length-offset;
        if(num<0) num =0;
        for(int i=0; i<num; ++i) data[i] = doubleValue[i+offset];
        return num;
    }
}
