/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Structure;


/**
 * Optional callback for PvaClientMonitor.
 * @author mrk
 * @since 2017.07
 */

public interface PvaClientMonitorRequester {
    /** 
     * The server has returned a message that the monitor is connected.
     *
     * @param status Completion status.
     * @param pvaClientMonitor The pvaClientMonitor
     * @param structure The structure defining the data.
     */
    public void monitorConnect(Status status,PvaClientMonitor pvaClientMonitor,Structure structure);
    /**
     * A monitor event has occurred.
     * @param pvaClientMonitor The PvaClientMonitor that trapped the event.
     */
    public void event(PvaClientMonitor pvaClientMonitor);
    /**
     * The data source is no longer available.
     * @param pvaClientMonitor The PvaClientMonitor that trapped the event.
     */
    public void unlisten(PvaClientMonitor pvaClientMonitor);
}
