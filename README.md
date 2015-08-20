pvaClientCPP
============

pvaClient is a synchronous client interface to pvAccess,
which is callback based.
pvaClient is thus easier to use than pvAccess itself.

Building
--------

If a proper RELEASE.local file exists one directory level above pvaClientCPP
then just type:

    make

If RELEASE.local does not exists the look at <b>configure/RELEASE</b>
for directions for how to build.

Examples
------------

The examples require the database in pvaClientTestCPP.
For example:

    mrk> pwd
    /home/epicsv4/pvaClientTestCPP/database/iocBoot/exampleDatabase
    mrk> ../../bin/linux-x86_64/exampleDatabase st.cmd 

Status
------

* The API should ready for feature freeze for release 4.5
* Everything defined in pvaClient.h should be ready but see below for remaining work.
* Everything defined in pvaClientMultiChannel.h is ready but see below for remaining work.


pvaClientChannel
---------------

Channel::getField and channelArray are not supported for release 4.5.

pvaClientMultiChannel
---------------

For release 4.6 support is available for multiDouble and NTMultiChannel.
In the future additional support should be provided that at least includes NTScalarMultiChannel.

Testing with some channels not connected have not been done.
At least some testing with missing channels should be done before release 4.5
