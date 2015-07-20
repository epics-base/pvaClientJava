/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvaccess.client.Channel;
import org.epics.pvdata.pv.PVStructure;
/**
 * An easy to use alternative to directly calling the Channel::getField.
 * NOT implemented.
 * @author mrk
 *
 */
public class PvaClientField
{
    /**
     * Create new PvaClientField.
     * NOT implemented.
     * @return The interface.
     */
    static PvaClientField create(
        PvaClient pvaClient,
        PvaClientChannel pvaClientChannel,
        Channel channel,
        PVStructure pvRequest)
    {
        throw new RuntimeException("pvaClientField not implemented");
    }
}
