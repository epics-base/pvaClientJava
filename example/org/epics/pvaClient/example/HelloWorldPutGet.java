/*HelloWorldPutGet.java */
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
import org.epics.pvaClient.PvaClientChannel;
import org.epics.pvaClient.PvaClientGetData;
import org.epics.pvaClient.PvaClientPutData;
import org.epics.pvaClient.PvaClientPutGet;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;


public class HelloWorldPutGet
{
    static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();

    static void exampleSimple(PvaClient pva)
    {
        System.out.println("helloWorldPutGet");
        try {
            PvaClientChannel channel = pva.channel("exampleHello");
            PvaClientPutGet putGet = channel.createPutGet();
            putGet.connect();
            PvaClientPutData putData = putGet.getPutData();
            PVStructure arg = putData.getPVStructure();
            PVString pvValue = arg.getSubField(PVString.class,"argument.value");
            pvValue.put("World");
            putGet.putGet();
            PvaClientGetData getData = putGet.getGetData();
            System.out.println(getData.getPVStructure());
        }  catch (RuntimeException e) {
            System.out.println("exception " + e.getMessage());
        }
    }

    public static void main( String[] args )
    {
        PvaClient pva= PvaClient.get();
        exampleSimple(pva);
    }

}
