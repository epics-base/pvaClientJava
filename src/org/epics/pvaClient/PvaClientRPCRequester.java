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

import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;

/**
 * Optional interface for a PvaClientMonitor requester.
 *
 */

public interface PvaClientRPCRequester {
    /**
     * The request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param channelRPC The pvaClientRPC interface.
     * @param pvResponse The response data for the RPC request or <code>null</code> if the request failed.
     */
    public void requestDone(
            Status status,
            PvaClientRPC channelRPC,
            PVStructure pvResponse);

}
