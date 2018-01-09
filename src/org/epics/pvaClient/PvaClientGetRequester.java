/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import org.epics.pvdata.pv.Status;


/**
 * Optional callback for PvaClientGet.
 * @author mrk
 * @since 2017.12
 */

public interface PvaClientGetRequester {
    /**
     * The client and server have both completed the createChannelGet request.
     * @param status Completion status.
     * @param pvaClientGet The PvaChannelGet interface.
     */
    public void channelGetConnect(Status status,PvaClientGet pvaClientGet);
    /**
     * The get request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientGet The PvaChannelGet interface.
     */
    public void getDone(Status status, PvaClientGet pvaClientGet);
}
