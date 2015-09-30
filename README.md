pvaClientJava
============

pvaClient is a synchronous client interface to pvAccess,
which is callback based.
pvaClient is thus easier to use than pvAccess itself.

See documentation/pvaClientJava.html for details.

Building
--------

    mvn package

Examples
------------

The examples require the database in pvaClientTestCPP.
For example:

    mrk> pwd
    /home/epicsv4/pvaClientTestCPP/database/iocBoot/exampleDatabase
    mrk> ../../bin/linux-x86_64/exampleDatabase st.cmd 

Status
------

* The API is for EPICS Version 4 release 4.5.0
* pvaClient should be ready but see below for remaining work.
* pvaClientMultiChannel is ready but see below for remaining work.


pvaClient
---------------

Channel::getField and channelArray are not supported for release 4.5.

pvaClientMultiChannel
---------------

For release 4.5 support is available for multiDouble and NTMultiChannel.
In the future additional support should be provided that at least includes NTScalarMultiChannel.

Testing with some channels not connected has not been done.
