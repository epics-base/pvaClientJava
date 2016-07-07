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

public interface PvaClientUnlistenRequester {
    /**
     * The data source is no longer available.
     * @param monitor The PvaClientMonitor that received the unlisten request.
     */
    public void unlisten(PvaClientMonitor monitor);
}
