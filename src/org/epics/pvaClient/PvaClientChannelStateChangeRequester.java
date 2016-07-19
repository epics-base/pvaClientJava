/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */

package org.epics.pvaClient;


/**
 * Optional  callback for change in connection status.
 * @author mrk
 * @since 2016.07
 */

public interface PvaClientChannelStateChangeRequester {
    /**
     * A channel connection state change has occurred.
     * @param channel The channel.
     * @param isConnected The new connection status.
     */
    public void channelStateChange(PvaClientChannel channel, boolean isConnected);
}
