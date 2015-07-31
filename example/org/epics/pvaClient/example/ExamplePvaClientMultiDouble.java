/*examplePvaClientMultiDouble.java */
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
import org.epics.pvaClient.PvaClientMultiGetDouble;
import org.epics.pvaClient.PvaClientMultiMonitorDouble;
import org.epics.pvaClient.PvaClientMultiPutDouble;

public class ExamplePvaClientMultiDouble
{
    static String printData(double[] data)
    {
        String ret = "";
        for(int i=0; i<data.length; ++i) {
            if(i>0)ret += ",";
            ret += data[i];
        }
        return ret;
    }
    static void example(
            PvaClient pva,
            String provider,
            String[] channelNames)
    {

        int num = channelNames.length;
        PvaClientMultiChannel multiChannel =
                PvaClientMultiChannel.create(pva,channelNames,provider,0);
        PvaClientMultiGetDouble multiGet = multiChannel.createGet();
        PvaClientMultiPutDouble multiPut =multiChannel.createPut();
        PvaClientMultiMonitorDouble multiMonitor = multiChannel.createMonitor();
        double[] data = new double[num];
        for(int i=0; i<num; ++i) data[i] = 0.0;
        for(double value = 0.0; value< 1.0; value+= .2) {
            try {
                for(int i=0; i<num; ++i) data[i] = value + i;
                System.out.println("put " + printData(data));
                multiPut.put(data);
                data =  multiGet.get();
                System.out.println("get " + printData(data));
                boolean result = multiMonitor.waitEvent(.1);
                while(result) {
                    System.out.println("monitor " + printData(data));
                    result = multiMonitor.poll();
                }
            } catch (RuntimeException e) {
                System.out.println("exception " + e.getMessage());
            }

        }
    }

    public static void main( String[] args )
    {
        PvaClient pva = PvaClient.get();
        int num = 5;
        String[] names = new String[num];
        names[0] = "double01";
        names[1] = "double02";
        names[2] = "double03";
        names[3] = "double04";
        names[4] = "double05";
        System.out.println("double pva");
        example(pva,"pva",names);
        System.out.println("double ca");
        example(pva,"ca",names);
        names[0] = "exampleDouble01";
        names[1] = "exampleDouble02";
        names[2] = "exampleDouble03";
        names[3] = "exampleDouble04";
        names[4] = "exampleDouble05";
        System.out.println("exampleDouble ca");
        example(pva,"pva",names);
    }
}
