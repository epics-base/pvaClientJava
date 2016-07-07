/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
/**
 * @author mrk
 * @date 2016.07
 */

package org.epics.pvaClient;


/**
 * Optional interface for a PvaClientMonitor requester.
 */

public interface PvaClientMonitorRequester {

    /**
     * A monitor event has occurred.
     * @param monitor The PvaClientMonitor that trapped the event.
     */
    public void event(PvaClientMonitor monitor);
}
