/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.*;

import org.epics.pvaccess.client.*;
/**
 * An easy to use alternative to directly calling the Channel::getField.
 * @author mrk
 *
 */
public class PvaClientField
{
    /**
     * Create new PvaClientField.
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
