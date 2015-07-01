/**
 * 
 */
package org.epics.pvaClient;


/**
 * Optional callback for client
 */
public interface PvaMultiMonitorRequester {
    
    /**
     * A monitor event has occurred.
     * @param monitor The EasyMonitor that traped the event.
     */
    void event(PvaClientMultiMonitor monitor);
}
