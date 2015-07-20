/*ExamplePvaClientProcess.java */
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

import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvaccess.server.rpc.RPCRequestException;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVDataCreate;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Structure;


public class HelloWorldRPC
{
    static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();
    static final PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();

    static void exampleSimple()
    {
        Structure  topStructure = fieldCreate.createFieldBuilder().
                add("value",ScalarType.pvString).
                createStructure();
        PVStructure pvRequest = pvDataCreate.createPVStructure(topStructure);
        PVString pvArgument = pvRequest.getSubField(PVString.class,"value");
        pvArgument.put("World");
        System.out.println("example channeRPC simple");
        try {
            RPCClient rpcClient = RPCClientFactory.create("exampleHelloRPC");
            PVStructure pvResult = rpcClient.request(pvRequest, 2.0);
            System.out.println(pvResult);
        } catch (RPCRequestException e) {
            System.out.println("exception " + e.getMessage());
        }
    }

    public static void main( String[] args )
    {
        exampleSimple();
    }

}
