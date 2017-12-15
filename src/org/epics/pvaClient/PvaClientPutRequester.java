/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import org.epics.pvdata.pv.Status;


/**
 * Optional callback for PvaClientPut.
 * @author mrk
 * @since 2017.12
 */

public interface PvaClientPutRequester {
    /**
     * The client and server have both completed the createChannelPut request.
     * @param status Completion status.
     * @param pvaClientPut The PvaChannelPut interface.
     */
    public void channelPutConnect(Status status,PvaClientPut pvaClientPut);
    /**
     * The get request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientPut The PvaChannelPut interface.
     */
    public void getDone(Status status, PvaClientPut pvaClientPut);
    /**
     * The put request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientPut The PvaChannelPut interface.
     */
    public void putDone(Status status, PvaClientPut pvaClientPut);
}
