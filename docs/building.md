---
title:  "Build Stratosphere"
---


In order to build Stratosphere you need its source code. Either download the source of a release or clone the git repository. In addition to that, you need Maven 3 and a JDK. Note that you can not build Stratosphere with Oracle JDK 6 due to a unresolved bug in the Java compiler. It works well with OpenJDK 6 and all Java 7 and 8 compilers.

To clone from git, enter:
```
git clone {{ site.FLINK_GITHUB_URL }}
```

The simplest way of building Stratosphere is by running:

```
mvn clean package -DskipTests
```

This instructs Maven (`mvn`) to first remove all existing builds (`clean`) and then create a new Stratosphere binary (`package`). The `-DskipTests` command prevents Maven from executing the unit tests.



## Build Stratosphere for a specific Hadoop Version

This section covers building Stratosphere for a specific Hadoop version. Most users do not need to do this manually.
The problem is that Stratosphere uses HDFS and YARN which are both from Apache Hadoop. There exist many different builds of Hadoop (from both the upstream project and the different Hadoop distributions). Typically errors arise with the RPC services. An error could look like this:

```
ERROR: The job was not successfully submitted to the nephele job manager:
    eu.stratosphere.nephele.executiongraph.GraphConversionException: Cannot compute input splits for TSV:
    java.io.IOException: Failed on local exception: com.google.protobuf.InvalidProtocolBufferException:
    Protocol message contained an invalid tag (zero).; Host Details :
```

### Background

The builds with Maven are controlled by [properties](http://maven.apache.org/pom.html#Properties) and <a href="http://maven.apache.org/guides/introduction/introduction-to-profiles.html">build profiles</a>.
There are two profiles, one for hadoop1 and one for hadoop2. When the hadoop2 profile is enabled, the system will also build the YARN client.
The hadoop1 profile is used by default. To enable the hadoop2 profile, set `-Dhadoop.profile=2` when building.
Depending on the profile, there are two Hadoop versions, set via properties. For "hadoop1", we use 1.2.1 by default, for "hadoop2" it is 2.2.0.

You can change these versions with the `hadoop-two.version` (or `hadoop-one.version`) property. For example `-Dhadoop-two.version=2.4.0`.


### Example for Cloudera Hadoop 5 Beta 2


```
mvn -Dhadoop.profile=2 -Pvendor-repos -Dhadoop.version=2.2.0-cdh5.0.0-beta-2 -DskipTests package
```

The commands in detail:

*  `-Dhadoop.profile=2` activates the Hadoop YARN profile of Stratosphere. This will enable all components of Stratosphere that are compatible with Hadoop 2.2
*  `-Pvendor-repos` is adding the Maven repositories of MapR, Cloudera and Hortonworks into your Maven build.
* `-Dhadoop.version=2.2.0-cdh5.0.0-beta-2` sets a special version of the Hadoop dependencies. Make sure that the specified Hadoop version is compatible with the profile you activated.

If you want to build HDFS for Hadoop 2 without YARN, use the following parameter:

```
-P!include-yarn
```

Some Cloudera versions (such as `2.0.0-cdh4.2.0`) require this, since they have a new HDFS version with the old YARN API.

Please post to the _Stratosphere mailinglist_(dev@flink.incubator.apache.org) or create an issue on [Jira]({{site.FLINK_ISSUES_URL}}), if you have issues with your YARN setup and Stratosphere.