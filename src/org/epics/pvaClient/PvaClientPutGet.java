/**
 * 
 */
package org.epics.pvaClient;

import org.epics.pvdata.property.Alarm;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvaccess.client.*;

/**
 * EasyPutGet is not implemented.
 * The following is a guess at the methods to be implemented.
 * @author mrk
 *
 */
public class PvaClientPutGet {

    /**
     * Create new PvaClientPutGet.
     * @return The interface.
     */
    static PvaClientPutGet create(
        PvaClient pvaClient,
        PvaClientChannel pvaClientChannel,
        Channel channel,
        PVStructure pvRequest)
    {
        throw new RuntimeException("pvaClientPutGet not implemented");
    }
}
