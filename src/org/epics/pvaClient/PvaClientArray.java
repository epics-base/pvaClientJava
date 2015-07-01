/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvdata.pv.PVArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvaccess.client.*;

/**
 * An easy to use alternative to directly calling the ChannelArray methods of pvAccess.
 * @author mrk
 *
 */
public class PvaClientArray
{
    /**
     * Create new PvaClientArray.
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
