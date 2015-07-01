/**
 * 
 */
package org.epics.pvaClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.epics.pvdata.property.*;
import org.epics.pvdata.factory.*;
import org.epics.pvdata.pv.*;
import org.epics.pvdata.misc.*;
import org.epics.pvdata.monitor.*;

import org.epics.pvaccess.client.*;

/**
 * An easy interface to channelProcess.
 * @author mrk
 *
 */
public class PvaClientProcess {
    /**
     * Create new PvaClientProcess.
     * @return The interface.
     */
    static PvaClientProcess create(
        PvaClient pvaClient,
        PvaClientChannel pvaClientChannel,
        Channel channel,
        PVStructure pvRequest)
    {
        throw new RuntimeException("pvaClientProcess not implemented");
    }
    /**
     * Destroy the EasyProcess.
     */
    void destroy();
    /**
     * Call issueConnect and then waitConnect.
     * @return (false,true) means (failure,success)
     */
    boolean connect();
    /**
     * Issue a connect request and return immediately.
     */
    void issueConnect();
    /**
     * Wait until connection completes or for timeout.
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    Status waitConnect();
    /**
     * Call issueProcess and then waitProcess.
     * @return (false,true) means (failure,success)
     */
    void process();
    /**
     * Issue a process request and return immediately.
     */
    void issueProcess();
    /**
     * Wait until process completes or for timeout.
     * If failure getStatus can be called to get reason.
     * @return (false,true) means (failure,success)
     */
    Status waitProcess();
}
