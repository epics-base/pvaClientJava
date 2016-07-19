
/*pvaClientArray.java*/
/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS pvData is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.pvaClient;

import org.epics.pvaccess.client.Channel;
import org.epics.pvdata.pv.PVStructure;

/**
 * An easy to use alternative to directly calling the ChannelArray methods of pvAccess.
 * NOT implemented.
 * @author mrk
 * @since 2015.06
 */
public class PvaClientArray
{
    /**
     * Create new PvaClientArray.
     * NOT implemented.
     * @return The interface.
     */
    static PvaClientArray create(
            PvaClient pvaClient,
            PvaClientChannel pvaClientChannel,
            Channel channel,
            PVStructure pvRequest)
    {
        throw new RuntimeException("pvaClientArray not implemented");
    }
}
