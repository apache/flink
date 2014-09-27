---
title:  "Build Flink"
---

In order to build Flink, you need the source code. Either download the source of a release or clone the git repository. In addition to that, you need Maven 3 and a JDK (Java Development Kit). Note that you can not build Flink with Oracle JDK 6 due to a unresolved bug in the Oracle Java compiler. It works well with OpenJDK 6 and all Java 7 and 8 compilers.

To clone from git, enter:

~~~bash
git clone {{ site.FLINK_GITHUB_URL }}
~~~

The simplest way of building Flink is by running:

~~~bash
cd incubator-flink
mvn clean package -DskipTests
~~~

This instructs Maven (`mvn`) to first remove all existing builds (`clean`) and then create a new Flink binary (`package`). The `-DskipTests` command prevents Maven from executing the unit tests.



## Build Flink for a specific Hadoop Version

This section covers building Flink for a specific Hadoop version. Most users do not need to do this manually.

The problem is that Flink uses HDFS and YARN which are both dependencies from Apache Hadoop. There exist many different versions of Hadoop (from both the upstream project and the different Hadoop distributions). If a user is using a wrong combination of versions, exceptions like this one occur:

~~~bash
ERROR: The job was not successfully submitted to the nephele job manager:
    org.apache.flink.nephele.executiongraph.GraphConversionException: Cannot compute input splits for TSV:
    java.io.IOException: Failed on local exception: com.google.protobuf.InvalidProtocolBufferException:
    Protocol message contained an invalid tag (zero).; Host Details :
~~~

There are two main versions of Hadoop that we need to differentiate:
- Hadoop 1, with all versions starting with zero or one, like 0.20, 0.23 or 1.2.1.
- Hadoop 2, with all versions starting with 2, like 2.2.0.
The main differentiation between Hadoop 1 and Hadoop 2 is the availability of Hadoop YARN (Hadoops cluster resource manager).

**To build Flink for Hadoop 2**, issue the following command:

~~~bash
mvn clean package -DskipTests -Dhadoop.profile=2
~~~

The `-Dhadoop.profile=2` flag instructs Maven to build Flink with YARN support and the Hadoop 2 HDFS client.

Usually, this flag is sufficient for full support of Flink for Hadoop 2-versions.
However, you can also **specify a specific Hadoop version to build against**:

~~~bash
mvn clean package -DskipTests -Dhadoop.profile=2 -Dhadoop.version=2.4.1
~~~


**To build Flink against a vendor specific Hadoop version**, issue the following command:

~~~bash
mvn clean package -DskipTests -Pvendor-repos -Dhadoop.profile=2 -Dhadoop.version=2.2.0-cdh5.0.0-beta-2
~~~

The `-Pvendor-repos` activates a Maven [build profile](http://maven.apache.org/guides/introduction/introduction-to-profiles.html) that includes the repositories of popular Hadoop vendors such as Cloudera, Hortonworks, or MapR.

**Build Flink for `hadoop2` versions before 2.2.0**

Maven will automatically build Flink with its YARN client if the `-Dhadoop.profile=2` is set. But there were some changes in Hadoop versions before the 2.2.0 Hadoop release that are not supported by Flink's YARN client. Therefore, you can disable building the YARN client with the following string: `-P\!include-yarn`. 

So if you are building Flink for Hadoop `2.0.0-alpha`, use the following command:

~~~bash
-P\!include-yarn -Dhadoop.profile=2 -Dhadoop.version=2.0.0-alpha
~~~

## Background

The builds with Maven are controlled by [properties](http://maven.apache.org/pom.html#Properties) and <a href="http://maven.apache.org/guides/introduction/introduction-to-profiles.html">build profiles</a>.
There are two profiles, one for hadoop1 and one for hadoop2. When the hadoop2 profile is enabled, the system will also build the YARN client.
The hadoop1 profile is used by default. To enable the hadoop2 profile, set `-Dhadoop.profile=2` when building.
Depending on the profile, there are two Hadoop versions, set via properties. For "hadoop1", we use 1.2.1 by default, for "hadoop2" it is 2.2.0.

You can change these versions with the `hadoop-two.version` (or `hadoop-one.version`) property. For example `-Dhadoop-two.version=2.4.0`.

