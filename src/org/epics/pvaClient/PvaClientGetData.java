/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.property.Alarm;
import org.epics.pvdata.property.PVAlarm;
import org.epics.pvdata.property.PVAlarmFactory;
import org.epics.pvdata.property.PVTimeStamp;
import org.epics.pvdata.property.PVTimeStampFactory;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.DoubleArrayData;
import org.epics.pvdata.pv.PVArray;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVScalarArray;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.StringArrayData;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.Type;

/**
 * This is a convenience wrapper for data for a channelGet or the get part of a channelPutGet.
 * @author mrk
 *
 */
public class PvaClientGetData {

    static PvaClientGetData create(Structure structure) {
        return new PvaClientGetData(structure);
    }

    private PvaClientGetData(Structure structure)
    {
        this.structure = structure;
    }

    private static final Convert convert = ConvertFactory.getConvert();
    private static final String noStructure ="no pvStructure ";
    private static final String noValue = "no value field";
    private static final String noScalar = "value is not a scalar" ;
    private static final String notCompatibleScalar = "value is not a compatible scalar" ;
    private static final String noArray = "value is not an array" ;
    private static final String noScalarArray = "value is not a scalarArray" ;
    private static final String notDoubleArray = "value is not a doubleArray" ;
    private static final String notStringArray = "value is not a stringArray" ;
    private static final String noAlarm = "no alarm" ;
    private static final String noTimeStamp = "no timeStamp" ;

    private final Structure structure;
    private PVStructure pvStructure = null;
    private BitSet bitSet = null;

    private String messagePrefix = "";
    private PVField pvValue;
    private final PVAlarm pvAlarm = PVAlarmFactory.create();
    private final Alarm alarm = new Alarm();
    private final PVTimeStamp pvTimeStamp = PVTimeStampFactory.create();
    private final TimeStamp timeStamp = TimeStampFactory.create();

    private final DoubleArrayData doubleArrayData = new DoubleArrayData();
    private final StringArrayData stringArrayData = new StringArrayData();

    private void checkValue()
    {
        if(pvValue!=null) return;
        throw new RuntimeException(messagePrefix + noValue);
    }

    /**
     * Set a prefix for throw messages.
     * @param value The prefix.
     */
    public void setMessagePrefix(String value)
    {
        messagePrefix = value + " ";
    }

    /** Get the structure.
     * @return the introspection interface for the data..
     */
    public Structure getStructure()
    {
        return structure;
    }

    /** Get the pvStructure.
     * @return the pvStructure.
     */
    public PVStructure getPVStructure()
    {
        if(pvStructure!=null) return pvStructure;
        throw new RuntimeException(messagePrefix + noStructure);
    }

    /** Get the BitSet for the pvStructure
     * This shows which fields have changed value.
     * @return The bitSet
     */
    public BitSet getChangedBitSet()
    {
        if(bitSet!=null) return bitSet;
        throw new RuntimeException(messagePrefix + noStructure);
    }

    /**
     * Show fields that have changed value, i. e. all fields as shown by bitSet.
     * @return The changed fields.
     */
    public String showChanged()
    {
        if(bitSet==null) throw new RuntimeException(messagePrefix + noStructure);
        String result = "";
        PVField pvField;
        int nextSet = bitSet.nextSetBit(0);
        while(nextSet>=0) {
            if(nextSet==0) {
                pvField = pvStructure;
            } else {
                pvField = pvStructure.getSubField(nextSet);
            }
            result += pvField.getFullName() + " = " + pvField + "\n";
            nextSet = bitSet.nextSetBit(nextSet+1);
        }
        return result;
    }

    /**
     * New data is present.
     * Called by pvaClientGet.
     * @param pvStructureFrom The new data.
     * @param bitSetFrom the bitSet showing which values have changed.
     */
    void setData(
            PVStructure pvStructureFrom,
            BitSet bitSetFrom)
    {
        pvStructure = pvStructureFrom;
        bitSet = bitSetFrom;
        pvValue = pvStructure.getSubField("value");
    }

    /**
     * Get the alarm for the last get.
     * @return The alarm.
     */
    public Alarm getAlarm()
    {
        if(pvStructure==null) throw new RuntimeException(messagePrefix + noStructure);
        PVStructure pvs = pvStructure.getSubField(PVStructure.class,"alarm");
        if(pvs==null) throw new RuntimeException(messagePrefix + noAlarm);
        pvAlarm.attach(pvs);
        if(pvAlarm.isAttached()) {
            pvAlarm.get(alarm);
            pvAlarm.detach();
            return alarm;
        }
        throw new RuntimeException(messagePrefix + noAlarm);
    }

    /**
     * Get the timeStamp for the last get.
     * @return The timeStamp.
     */
    public TimeStamp getTimeStamp()
    {
        if(pvStructure==null) throw new RuntimeException(messagePrefix + noStructure);
        PVStructure pvs = pvStructure.getSubField(PVStructure.class,"timeStamp");
        if(pvs==null) throw new RuntimeException(messagePrefix + noTimeStamp);
        pvTimeStamp.attach(pvs);
        if(pvTimeStamp.isAttached()) {
            pvTimeStamp.get(timeStamp);
            pvTimeStamp.detach();
            return timeStamp;
        }
        throw new RuntimeException(messagePrefix + noTimeStamp);
    }

    /**
     * Is there a top level field named value of the PVstructure returned by channelGet?
     * @return The answer.
     */
    public boolean hasValue()
    {
        if(pvValue==null) return false;
        return true;
    }

    /**
     * Is the value field a scalar?
     * @return The answer.
     */
    public boolean isValueScalar()
    {
        if(pvValue==null) return false;
        if(pvValue.getField().getType()==Type.scalar) return true;
        return false;
    }

    /**
     * Is the value field a scalar array?
     * @return The answer.
     */
    public boolean isValueScalarArray()
    {
        if(pvValue==null) return false;
        if(pvValue.getField().getType()==Type.scalarArray) return true;
        return false;
    }

    /**
     * Return the interface to the value field.
     * @return The interface or null if no top level value field.
     */
    public PVField getValue()
    {
        checkValue();
        return pvValue;
    }

    /**
     * Return the interface to a scalar value field.
     * @return Return the interface for a scalar value field or null if no scalar value field.
     */
    public PVScalar getScalarValue()
    {
        checkValue();
        PVScalar pv = pvStructure.getSubField(PVScalar.class,"value");
        if(pv==null) throw new RuntimeException(messagePrefix  + noScalar);
        return pv;
    }

    /**
     * Return the interface to an array value field.
     * @return Return the interface or null if an array value field does not exist.
     */
    public PVArray getArrayValue()
    {

        checkValue();
        PVArray pv = pvStructure.getSubField(PVArray.class,"value");
        if(pv==null) throw new RuntimeException(messagePrefix  + noArray);
        return pv;
    }

    /**
     * Return the interface to a scalar array value field.
     * @return Return the interface or null if a scalar array value field does not exist
     */
    public PVScalarArray getScalarArrayValue()
    {
        checkValue();
        PVScalarArray pv = pvStructure.getSubField(PVScalarArray.class,"value");
        if(pv==null) throw new RuntimeException(messagePrefix  + noScalarArray);
        return pv;
    }

    /**
     * Get the value as a double.
     * @return  If value is not a numeric scalar an exception is thrown.
     */
    public double getDouble()
    {
        PVScalar pvScalar = getScalarValue();
        ScalarType scalarType = pvScalar.getScalar().getScalarType();
        if(scalarType==ScalarType.pvDouble) {
            PVDouble pv = (PVDouble)pvScalar;
            return pv.get();

        }
        if(!scalarType.isNumeric()) throw new RuntimeException(
                messagePrefix  + notCompatibleScalar);
        return convert.toDouble(pvScalar);
    }

    /**
     * Get the value as a string.
     * @return The value.
     */
    public String getString()
    {
        PVScalar pvScalar = getScalarValue();
        return convert.toString(pvScalar);
    }

    /**
     * Get the value as a double array.
     * @return If the value is not a numeric array an exception is thrown. 
     */
    public double[] getDoubleArray()
    {
        checkValue();
        PVDoubleArray pvDoubleArray = pvStructure.getSubField(
                PVDoubleArray.class,"value");
        if(pvDoubleArray==null) throw new RuntimeException(
                messagePrefix  + notDoubleArray);
        int length = pvDoubleArray.getLength();
        double[] data = new double[length];
        pvDoubleArray.get(0, length, doubleArrayData);
        for(int i=0; i<length; ++i) data[i] = doubleArrayData.data[i];
        return data;
    }

    /**
     * Get the value as a string array.
     * @return If the value is not a scalar array an exception is thrown.
     */
    public String[] getStringArray()
    {
        checkValue();
        PVStringArray pvStringArray = pvStructure.getSubField(
                PVStringArray.class,"value");
        if(pvStringArray==null) throw new RuntimeException(
                messagePrefix  + notStringArray);
        int length = pvStringArray.getLength();
        String[] data = new String[length];
        pvStringArray.get(0, length, stringArrayData);
        for(int i=0; i<length; ++i) data[i] = stringArrayData.data[i];
        return data;
    }

    /**
     * Copy a sub-array of the value field into value.
     * If the value field is not a numeric array field no elements are copied.
     * @param value The place where data is copied.
     * @param length The maximum number of elements to copy.
     * @return The number of elements copied.
     */
    public int getDoubleArray(double[] value,int length)
    {
        checkValue();
        PVDoubleArray pvDoubleArray = pvStructure.getSubField(
                PVDoubleArray.class,"value");
        if(pvDoubleArray==null) throw new RuntimeException(
                messagePrefix  + notDoubleArray);
        if(pvDoubleArray.getLength()<length) length 
        = pvDoubleArray.getLength();            
        pvDoubleArray.get(0, length, doubleArrayData);
        for(int i=0; i<length; ++i) value[i] = doubleArrayData.data[i];
        return length;
    }

    /**
     * Copy a sub-array of the value field into value.
     * If the value field is not a scalar array field an exception is thrown.
     * @param value The place where data is copied.
     * @param length The maximum number of elements to copy.
     * @return The number of elements copied.
     */
    public int getStringArray(String[] value,int length)
    {
        checkValue();
        PVStringArray pvStringArray = pvStructure.getSubField(
                PVStringArray.class,"value");
        if(pvStringArray==null) throw new RuntimeException(
                messagePrefix  + notStringArray);
        if(pvStringArray.getLength()<length) length
        = pvStringArray.getLength();            
        pvStringArray.get(0, length, stringArrayData);
        for(int i=0; i<length; ++i) value[i] = stringArrayData.data[i];
        return length;
    }

}
