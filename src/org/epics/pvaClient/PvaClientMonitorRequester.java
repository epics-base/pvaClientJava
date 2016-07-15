/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;


/**
 * Interface for a PvaClientMonitorRequester.
 * @author mrk
 * @since 2017.07
 */

public interface PvaClientMonitorRequester {

    /**
     * A monitor event has occurred.
     * @param monitor The PvaClientMonitor that trapped the event.
     */
    public void event(PvaClientMonitor monitor);
}
