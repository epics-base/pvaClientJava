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

import org.epics.pvaClient.PvaClient;
import org.epics.pvaClient.PvaClientChannel;
import org.epics.pvaClient.PvaClientPut;
import org.epics.pvaClient.PvaClientPutData;


public class ExamplePvaClientPut
{


    static void exampleDouble(PvaClient pva)
    {
        System.out.println("example put via provider pva");
        PvaClientChannel channel = pva.channel("exampleDouble");
        PvaClientPut put = channel.put();
        PvaClientPutData putData = put.getData();
        try {
            putData.putDouble(3.0); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
            putData.putDouble(4.0); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
        } catch (RuntimeException e) {
            System.out.println("exception " + e.getMessage());
        }
    }
    
    static void exampleDoubleArray(PvaClient pva)
    {
        System.out.println("example put array via provider pva");
        PvaClientChannel channel = pva.channel("exampleDoubleArray");
        PvaClientPut put = channel.put();
        PvaClientPutData putData = put.getData();
        try {
            double[] data = new double[5];
            for(int i=0; i< data.length; ++i) data[i] = .1*i;
            putData.putDoubleArray(data); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
            for(int i=0; i< data.length; ++i) data[i] = .1*(i+1);
            putData.putDoubleArray(data); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
        } catch (RuntimeException e) {
            System.out.println("exception " + e.getMessage());
        }
    }
    
    static void exampleCADouble(PvaClient pva)
    {
        System.out.println("example put via provider ca");
        PvaClientChannel channel = pva.channel("double00","ca",5.0);
        PvaClientPut put = channel.put();
        PvaClientPutData putData = put.getData();
        try {
            putData.putDouble(3.0); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
            putData.putDouble(4.0); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
        } catch (RuntimeException e) {
            System.out.println("exception " + e.getMessage());
        }
    }
    
    static void exampleCADoubleArray(PvaClient pva)
    {
        System.out.println("example put array via provider ca");
        PvaClientChannel channel = pva.channel("doubleArray","ca",5.0);
        PvaClientPut put = channel.put();
        PvaClientPutData putData = put.getData();
        try {
            double[] data = new double[5];
            for(int i=0; i< data.length; ++i) data[i] = .1*i;
            putData.putDoubleArray(data); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
            for(int i=0; i< data.length; ++i) data[i] = .1*(i+1);
            putData.putDoubleArray(data); put.put();
            System.out.println(channel.get("field()").getData().showChanged());
        } catch (RuntimeException e) {
            System.out.println("exception " + e.getMessage());
        }
    }

    public static void main( String[] args )
    {
        PvaClient pva= PvaClient.get();
        exampleDouble(pva);
        exampleDoubleArray(pva);
        exampleCADouble(pva);
        exampleCADoubleArray(pva);
    }

}
