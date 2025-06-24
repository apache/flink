---
title: Delegation tokens
weight: 4
type: docs
aliases:
  - /zh/deployment/security/security-delegation-token.html
  - /zh/ops/security-delegation-token.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Delegation Tokens

This document aims to explain and demystify delegation tokens as they are used by Flink.
Before we into the details here is the high level architecture diagram:

{{< img src="/fig/delegation_token_framework.svg" alt="High Level Architecture Diagram" >}}

## What Are Delegation Tokens and Why Use Them?

Delegation tokens (DTs from now on) are authentication tokens used by some services to replace
long-lived credentials. Many services in the Hadoop ecosystem have support for DTs, since they
have some very desirable advantages over long-lived credentials:

### No need to distribute long-lived credentials

In a distributed application, distributing long-lived credentials is tricky. Additionally, not all
users wants to distribute them over the network as part of application data as it is an additional 
attack surface for malicious actors.

DTs allow for a single place e.g. the JobManager (JM from now on) to require long-lived credentials.
That entity can then distribute the DTs to other parts of the distributed application e.g. TaskManagers
(TM from now on), so they can authenticate to services.

### A single token per service is used for authentication

If [Kerberos authentication](https://web.mit.edu/kerberos/) were used, each client connection 
to a server would require a trip to the Key Distribution Center (KDC) and generation of a service ticket. 
In a distributed system, the number of service tickets can balloon pretty quickly due in proportion 
to the number of client processes (e.g. TMs) times the number of service processes (e.g. HDFS DataNodes). 
That generates unnecessary extra load on the KDC, and may even run into usage limits set up by the KDC admin.

### Delegation tokens are only used for authentication

DTs, unlike long-lived credentials, can only be used to authenticate to the specific service for
which they were issued. You cannot use an existing DT to create new DTs or to create DTs for a different service.

So in short, DTs are **not** long-lived credentials. They are used by many services to replace Kerberos
authentication, or even other forms of authentication, although there is nothing (aside from
maybe implementation details) that ties them to the authentication mechanism.

## Lifecycle of Delegation Tokens

DTs, unlike some long-lived credentials, are service-specific. There is no centralized location you
contact to create a DT for a service. So, the first step needed to get a DT is being able to authenticate
to the service in question. In the Hadoop ecosystem, that is generally done using Kerberos.

This requires long-lived credentials to be available somewhere for the application to use. The user
is generally responsible for providing those credentials, which is most commonly done by logging
in to the KDC (e.g. using `kinit`). That generates a "credential cache" containing a ticket granting
ticket (TGT), which can then be used to request service tickets.

There are other ways of obtaining TGTs, but, ultimately, one needs a TGT to bootstrap the process.

Once a TGT is available, the target service's client library can then be used to authenticate
to the service and request the creation of a delegation token.
This token can now be sent to other processes and used to authenticate to different daemons
belonging to that service. And thus the first drawback of DTs becomes apparent: 
you need service-specific logic to create and use them.

Flink implements a (somewhat) pluggable, internal DT creation API. Support for new services can be
added by implementing a `DelegationTokenProvider` that is then called by the delegation token manager
when generating delegation tokens for an application. 

Once they are created, the semantics of how DTs operate are also service-specific. But, in general,
they try to follow the semantics of Kerberos tokens:

* A "renewable period" (equivalent to TGT's "lifetime") stands for the DT's validity length
  before it requires renewal.
* A "max lifetime" (equivalent to TGT's "renewable life") stands for the time until the DT can be
  renewed.

Once the token reaches its "max lifetime", a new one needs to be created by contacting the
appropriate service, restarting the above process.

## Delegation Token Renewal and Renewers

This is the most confusing part of DT handling, and part of it is because much of the system was
designed with [Apache Hadoop YARN](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html) 
in mind even though it extends to other services and mechanism.

As seen above, DTs need to be renewed periodically until they finally expire for good. An example of
this is the default configuration of HDFS services: delegation tokens are valid for up to 7 days,
and need to be renewed every 24 hours. If 24 hours pass without the token being renewed, the token
cannot be used anymore. And the token cannot be renewed anymore after 7 days.

This raises the question: who renews tokens? And for a long time the answer was YARN.

When YARN applications are submitted, a set of DTs is also submitted with them. YARN takes care
of distributing these tokens to containers (using conventions set by the `UserGroupInformation` API) 
and, also, keeping them renewed while the app is running. These tokens are used not just by the application,
they are also used by YARN itself to implement features like log collection and aggregation.

But this has a few caveats.

### Who renews the tokens?

This is handled mostly transparently by the Hadoop libraries in the case of YARN. Some services have
the concept of a token "renewer". This "renewer" is the name of the service principal that is allowed to
renew the DT. When submitting to YARN, that will be the principal that the YARN service is running
as, which means that the client application needs to know that information.

For other resource managers, the renewer mostly does not matter, since there is no service that
is doing the renewal.

### Which tokens are renewed?

This is probably the biggest caveat.

As discussed in the previous section, DTs are service-specific, and require service-specific
libraries for creation *and* renewal. This means that for YARN to be able to renew application
tokens, YARN needs:

* The client libraries for all the services the application is using
* Information about how to connect to the services the application is using
* Permissions to connect to those services

In reality, though, most of the time YARN has access to a single HDFS cluster, and that will be
the extent of its DT renewal features. Any other tokens sent to YARN will be distributed to
containers, but will not be renewed.

This means that those tokens will expire way before their max lifetime, unless some other code
takes care of renewing them.

Also, not all client libraries even implement token renewal. To use the example of a service
supported by Flink, the `renew()` method of HBase tokens is a no-op. So the only way to "renew" an
HBase token is to create a new one.

### What happens when tokens expire for good?

The final caveat is that DTs have a maximum life, regardless of renewal. And after that deadline
is met, you need to create new tokens to be able to connect to the services. That means you need
the ability to connect to the service without a delegation token, which requires some form of
authentication aside from DTs.

This is especially important for long-running applications that run unsupervised. They must be
able to keep on going without having someone logging into a terminal and typing a password every
few days.

## Delegation Token Renewal

Because of the issues explained above, Flink implements a different way of doing renewal. The
solution is a compromise: it targets the lowest common denominator, which is services like HBase
that do not support actual token renewal. In the following examples we often drive the discussion via
highlighting the Hadoop ecosystem components to cover all our bases, because they tend to be more 
complex from an authentication perspective compared to others e.g. [AWS S3](https://aws.amazon.com/s3/).

In Flink, DT "renewal" is enabled by giving the application long-lived credentials (e.g. keytab).
A keytab is equivalent to your Kerberos password written into a plain text file, which is why it is so
sensitive: if anyone is able to get hold of that keytab file, they can authenticate to any service as 
that user as long as the credentials stored in the keytab remain valid in the KDC.

By having the keytab, Flink can indefinitely maintain a valid Kerberos TGT.

With long-lived credentials available, Flink will create new DTs for the configured services as old
ones expire. So Flink doesn't renew tokens as explained in the previous section: it will create new
tokens at every renewal interval instead, and distribute those tokens to TMs.

This also has another advantage on top of supporting services like HBase: it removes the dependency
on an external renewal service (like YARN). That way, Flink's renewal feature can be used with
resource managers that are not DT-aware, such as Kubernetes, as long as the application has
long-lived credentials.

## Delegation Tokens and Proxy Users

"Proxy users" is Hadoop-speak for impersonation. It allows user A to impersonate user B when
connecting to a service, if that service allows it.

Flink simply doesn't allow impersonation when submitting applications. Spark supports impersonation
but doesn't allow token renewal. Since Flink is mainly designed for streaming workloads there
would be not much gain to add this feature.

## Externally Generated Delegation Tokens

Flink uses the `UserGroupInformation` (`UGI`) API to manage the Hadoop credentials. That means that Flink
inherits the feature of loading DTs automatically from a file. The Hadoop classes will load the
token cache pointed at by the `HADOOP_TOKEN_FILE_LOCATION` environment variable, when it's defined.

This feature is mostly used by services that start workloads on behalf of users. Regular users rarely
use this feature, given it would require them to figure out how to get those tokens outside Flink.

Note that Flink itself can obtain DTs. In case `UGI` contains a DT for a specific
service and Flink is configured to obtain tokens for that service then the token first will be
loaded and then will be overwritten by the loading mechanism in Flink.

## Limitations of Delegation Token Support

There are certain limitations to bear in mind when talking about DTs.

Firstly, not all DTs actually expose their renewal period. This is a service configuration that is 
not generally exposed to clients. For this reason, certain DT providers cannot provide a renewal period, 
thus requiring that the service's configuration is in some way synchronized with another service
that does provide that information.

The HDFS service, which is generally available when DTs are needed in the first place, provides
this information, so in general it's a good idea for all services using DTs to use the same
configuration as HDFS for the renewal period.

Secondly, Flink is not parsing the user application code, so it doesn't know which delegation
tokens will be needed. This means that Flink will try to get as many delegation tokens as is possible
based on the configuration available. That means that if an HBase token provider is enabled but the app
doesn't actually use HBase, a DT will still be generated. The user would have to explicitly
disable the mentioned provider in that case.

Thirdly, it is challenging to create DTs "on demand". Flink obtains/distributes tokens upfront
and re-obtains/re-distributes them periodically.

The advantage, though, is that user code does not need to worry about DTs, since Flink will handle
them transparently when the proper configuration is available.
