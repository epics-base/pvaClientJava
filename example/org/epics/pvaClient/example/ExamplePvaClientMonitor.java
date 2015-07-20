/*ExamplePvaClientMonitor.java */
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


public class ExamplePvaClientMonitor
{


    static void exampleMonitor(PvaClient pva)
    {
        PvaClientMonitor monitor = pva.channel("exampleDouble").monitor("");
        PvaClientMonitorData pvaData = monitor.getData();
        PvaClientPut put = pva.channel("exampleDouble").put("");
        PvaClientPutData putData = put.getData();
        for(int ntimes=0; ntimes<5; ++ntimes)
        {
            try {
                double value = ntimes;
                putData.putDouble(value); put.put();
                if(!monitor.waitEvent(0.0)) {
                    System.out.println("waitEvent returned false. Why???");
                }
                System.out.println("changed");
                System.out.println(pvaData.showChanged());
                System.out.println("overrun");
                System.out.println(pvaData.showOverrun());
                monitor.releaseEvent();
            } catch (RuntimeException e) {
                System.out.println("exception " + e.getMessage());
            }
        }
    }

    public static void main( String[] args )
    {
        PvaClient pva= PvaClient.get();
        exampleMonitor(pva);
    }

}
