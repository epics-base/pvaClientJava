/*ExamplePvaClientNTMulti.java */
/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
/**
 * @author mrk
 */

/* Author: Marty Kraimer */
package org.epics.pvaClient.example;

import org.epics.pvaClient.PvaClient;
import org.epics.pvaClient.PvaClientMultiChannel;
import org.epics.pvaClient.PvaClientNTMultiData;
import org.epics.pvaClient.PvaClientNTMultiGet;
import org.epics.pvaClient.PvaClientNTMultiMonitor;
import org.epics.pvaClient.PvaClientNTMultiPut;
import org.epics.pvdata.factory.ConvertFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.Convert;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.Scalar;
import org.epics.pvdata.pv.ScalarArray;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Type;
import org.epics.pvdata.pv.Union;

public class ExamplePvaClientNTMulti
{
    private static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    private static final Convert convert = ConvertFactory.getConvert();



    static void setValue(PVUnion pvUnion, double value)
    {
        Union u = pvUnion.getUnion();
        Field field = u.getField(0);
        Type type = field.getType();
        if(type==Type.scalar) {
            Scalar scalar = (Scalar)field;
            ScalarType scalarType = scalar.getScalarType();
            if(scalarType==ScalarType.pvDouble) {
                PVDouble pvValue = (PVDouble)(
                        pvDataCreate.createPVScalar(ScalarType.pvDouble));
                pvValue.put(value);
                pvUnion.set(0,pvValue);
                return;
            }
            if(scalarType==ScalarType.pvString) {
                PVString pvValue = (PVString)(
                        pvDataCreate.createPVScalar(ScalarType.pvString));
                String ss = "value " + value;
                pvValue.put(ss);
                pvUnion.set(0,pvValue);
                return;
            }
        }
        if(type==Type.scalarArray) {
            ScalarArray scalarArray = (ScalarArray)field;
            ScalarType scalarType = scalarArray.getElementType();
            if(scalarType==ScalarType.pvDouble) {
                int num = 5;
                PVDoubleArray pvValue = (PVDoubleArray)(
                        pvDataCreate.createPVScalarArray(ScalarType.pvDouble));
                double[] data = new double[num];
                for(int i=0; i<num; ++i) data[i] = value +i;
                convert.fromDoubleArray(pvValue, 0, num, data, 0);
                pvUnion.set(0,pvValue);
                return;
            }
            if(scalarType==ScalarType.pvString) {
                int num = 5;
                PVStringArray pvValue = (PVStringArray)(
                        pvDataCreate.createPVScalarArray(ScalarType.pvString));
                String[] data = new String[num];
                for(int i=0; i<num; ++i) {
                    String ss = "value " + value + i;
                    data[i] = ss;
                }
                convert.fromStringArray(pvValue, 0, num, data, 0);
                pvUnion.set(0,pvValue);
                return;
            }

        }
        throw new RuntimeException("only pvDouble and pvString are supported");
    }

    static void example(
            PvaClient pva,
            String provider,
            String[] channelNames)
    {
        int num = channelNames.length;
        PvaClientMultiChannel multiChannel =
                PvaClientMultiChannel.create(pva,channelNames,provider,0);
        PvaClientNTMultiGet multiGet = multiChannel.createNTGet();
        PvaClientNTMultiPut multiPut =multiChannel.createNTPut();
        PvaClientNTMultiMonitor multiMonitor = multiChannel.createNTMonitor();
        PVUnion[] data = multiPut.getValues();
        for(double value = 0.0; value< 2.1; value+= 1.0) {
            for(int i=0; i<num ; ++i) {
                PVUnion pvUnion = data[i];
                setValue(pvUnion,value);
            }
            multiPut.put();
            multiGet.get();
            PvaClientNTMultiData multiData = multiGet.getData();
            PVStructure pvStructure = multiData.getNTMultiChannel().getPVStructure();
            System.out.println("pvStructure");
            System.out.println(pvStructure);
            boolean result = multiMonitor.waitEvent(.1);
            while(result) {
                multiData = multiMonitor.getData();
                pvStructure = multiData.getNTMultiChannel().getPVStructure();
                System.out.println("monitor pvStructure");
                System.out.println(pvStructure);
                result = multiMonitor.poll();
            }
        }
    }

    public static void main( String[] args )
    {
        PvaClient pva = PvaClient.get();
        int num = 4;
        String[] channelNames = new String[num];
        channelNames[0] = "double01";
        channelNames[1] = "string01";
        channelNames[2] = "doubleArray01";
        channelNames[3] = "stringArray01";
        System.out.println("dbRecord pva");
        example(pva,"pva",channelNames);
        System.out.println("dbRecord ca");
        example(pva,"ca",channelNames);
        channelNames[0] = "exampleDouble";
        channelNames[1] = "exampleString";
        channelNames[2] = "exampleDoubleArray";
        channelNames[3] = "exampleStringArray";
        System.out.println("dbRecord pva");
        example(pva,"pva",channelNames);
    }
}
