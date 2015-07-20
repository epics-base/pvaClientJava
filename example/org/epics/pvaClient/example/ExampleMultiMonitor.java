package org.epics.pvaClient.example;

import org.epics.pvaClient.PvaClient;
import org.epics.pvaClient.PvaClientMultiChannel;
import org.epics.pvaClient.PvaClientMultiMonitor;
import org.epics.pvaClient.PvaClientMultiPut;

public class ExampleMultiMonitor
{

	static PvaClient pvaClient= PvaClient.get();

	public static void main( String[] args )
	{
	    int nchannel = 5;
        String[] channelName = new String[nchannel];
        for(int i=0; i<nchannel; ++i) channelName[i] = "double0" + i;
        PvaClientMultiChannel multiChannel = PvaClientMultiChannel.create(pvaClient, channelName,"ca");
        multiChannel.connect(5.0,0);
	    PvaClientMultiPut pvaClientMultiPut = multiChannel.createPut(true);
	    double[] value = new double[nchannel];
	    for(int i=0; i<nchannel; ++i) value[i] = i+1;
	    pvaClientMultiPut.put(value);
	    PvaClientMultiMonitor monitor = multiChannel.createMonitor(true);
	    monitor.start(0.0);
        for(int ntimes=0; ntimes<3; ++ntimes)
        {
            try {
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
                int numMonitor =  monitor.poll();
                System.out.println("numMonitor " + numMonitor);
                while(numMonitor>0) {
                    double[] data = monitor.getDoubleArray();
                    System.out.printf("received ");
                    for (int i=0; i<data.length; ++i) System.out.printf(" %f",data[i]);
                    System.out.println();
                    if(monitor.release()) {
                        numMonitor =  monitor.poll();
                    } else {
                        numMonitor = 0;
                    }
                }
                for(int i=0; i<nchannel; ++i) value[i] += 1.0;
                pvaClientMultiPut.put(value);
                
            } catch (RuntimeException e) {
                System.out.println("exception " + e.getMessage());
            }
        }
	}
}
