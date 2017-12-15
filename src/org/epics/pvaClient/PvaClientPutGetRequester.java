/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;

import org.epics.pvdata.pv.Status;


/**
 * Optional callback for PvaClientPutGet.
 * @author mrk
 * @since 2017.12
 */

public interface PvaClientPutGetRequester {
    /**
     * The client and server have both completed the createChannelPut request.
     * @param status Completion status.
     * @param pvaClientPutGet The PvaChannelPutGet interface.
     */
    public void channelPutGetConnect(Status status,PvaClientPutGet pvaClientPutGet);
    /**
     * The putGet request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientPutGet The PvaChannelPutGet interface.
     */
    public void putGetDone(Status status, PvaClientPutGet pvaClientPutGet);
    /**
     * The getPut request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientPutGet The PvaChannelPutGet interface.
     */
    public void getPutDone(Status status, PvaClientPutGet pvaClientPutGet);
    /**
     * The getGet request is done. This is always called with no locks held.
     * @param status Completion status.
     * @param pvaClientPutGet The PvaChannelPutGet interface.
     */
    public void getGetDone(Status status, PvaClientPutGet pvaClientPutGet);
}
