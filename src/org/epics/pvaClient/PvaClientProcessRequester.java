/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import org.epics.pvdata.pv.Status;


/**
 * Optional callback for PvaClientProcess.
 * @author mrk
 * @since 2017.12
 */

public interface PvaClientProcessRequester {
    /**
     * The client and server have both completed the createChannelProcess request.
     * @param status Completion status.
     * @param pvaClientProcess The pvaClientProcess interface.
     */
    public void channelProcessConnect(Status status,PvaClientProcess pvaClientProcess);
    /**
     * The process request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientProcess The pvaClientProcess interface.
     */
    public void processDone(Status status, PvaClientProcess pvaClientProcess);
}
