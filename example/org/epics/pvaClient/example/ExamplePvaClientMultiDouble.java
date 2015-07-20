/*ExamplePvaClientPut.java */
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

import org.epics.pvaClient.*;
import org.epics.pvaClient.PvaClientMultiChannel;
import org.epics.pvaClient.PvaClientMultiGet;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.PVDataCreate;


public class ExamplePvaClientMultiDouble
{

    static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();
    
    static void exampleCADouble(PvaClient pvaClient)
    {
        System.out.println("example multiDouble");
        int num = 5;
        String[] channelNames = new String[num];
        channelNames[0] = "double01";
        channelNames[1] = "double02";
        channelNames[2] = "double03";
        channelNames[3] = "double04";
        channelNames[4] = "double05";
        PvaClientMultiChannel multiChannel = PvaClientMultiChannel.create(pvaClient, channelNames,"ca");
        multiChannel.connect(5.0,0);
        PvaClientMultiGet pvaClientMultiGet = multiChannel.createGet(true);
        PvaClientMultiPut pvaClientMultiPut = multiChannel.createPut(true);
        try {
            pvaClientMultiGet.get();
            double[] value = pvaClientMultiGet.getDoubleArray();
            System.out.println("initial");
            for(int i=0 ; i< value.length; ++i) System.out.println(value[i] + " ");
            System.out.println();
            for(int i=0 ; i< value.length; ++i) value[i] += 1.0;
            pvaClientMultiPut.put(value);
            value = pvaClientMultiGet.getDoubleArray();
            System.out.println("after put");
            for(int i=0 ; i< value.length; ++i) System.out.println(value[i] + " ");
            System.out.println();
        } catch (RuntimeException e) {
            System.out.println("exception " + e.getMessage());
        }
    }

    public static void main( String[] args )
    {
        PvaClient pva= PvaClient.get();
        exampleCADouble(pva);
    }

}
