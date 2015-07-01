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
     * @param monitor The EasyMonitor that trapped the event.
     */
    void event(PvaClientMonitorRequester monitor);
}
