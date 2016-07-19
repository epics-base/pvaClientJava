EPICS V4 release 4.6
===========

* The examples are moved to exampleCPP.
* Support for channelRPC is now available.

EPICS V4 release 4.5
==========================
pvaClient is a synchronous API for pvAccess.

pvaClientJava is the successor to easyPVAJava.
The main difference is that pvaClient uses exceptions to report most problem instead
of requiring the client to call status methods.

EasyPVAJava: Release 0.4 IN DEVELOPMENT
---------------------------------------

The main changes since release 0.3.0

* EasyPVA automatically starts ChannelProvider for both Channel Access and pvAccess.
* EasyMultiChannel is now implemented
* Support for monitors is now available both for EasyChannel and EasyMultiChannel.


EasyPVA automatically starts Channel Providers
----------------------------------------------

It is no longer necessary to start channel provider factories.
There are two providers automatically started: pva and ca.

Both createChannel or createMultiChannel have two version; one uses the default provider
and the other allows the caller to specify the provider.
The default provider is "pva", which uses the pvAccess network protocol.
See pvAccessJava.html for details.
The other provider is "ca", which uses the Channel Access network protocol,
which can be used to communicate with EPICS IOCs that have no EPICS Version 4
software installed.

EasyMultiChannel is now implemented.
------------------------------------

Get and Put are available for accessing a set of channels.
See easyPVA.html for details.


EasyMonitor and EasyMultiLonitor are now implemented.
-----------------------------------------------------S

See easyPVA.html for details.


Release 0.3.0
==========
This was the starting point for RELEASE_NOTES
