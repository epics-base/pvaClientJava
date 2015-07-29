/**
 * 
 */
package org.epics.pvaClient;


/**
 * Optional interface for a PvaClientMonitor requester.
 * @author mrk
 *
 */

public interface PvaClientMonitorRequester {

    /**
     * A monitor event has occurred.
     * @param monitor The PvaClientMonitor that trapped the event.
     */
    public void event(PvaClientMonitor monitor);
}
