/**
 * 
 */
package org.epics.pvaClient;


/**
 * Optional interface for a PvaClientMonitor requester.
 * @author mrk
 *
 */

public interface PvaClientMultiMonitorRequester {

    /**
     * A monitor event has occurred.
     * @param monitor The PvaClientMultiMonitor that trapped the event.
     */
    public void event(PvaClientMultiMonitor monitor);
}
