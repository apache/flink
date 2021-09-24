---
title: Building Flink from Source
weight: 21
type: docs
aliases:
  - /flinkDev/building.html
  - /start/building.html
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

# Building Flink from Source

This page covers how to build Flink {{< version >}} from sources.

## Build Flink

In order to build Flink you need the source code. Either [download the source of a release]({{< downloads >}}) or [clone the git repository]({{< github_repo >}}).

In addition you need **Maven 3** and a **JDK** (Java Development Kit). Flink requires **at least Java 8** to build.

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.2.5 creates the libraries properly.
To build unit tests use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

To clone from git, enter:

```bash
git clone {{< github_repo >}}
```

The simplest way of building Flink is by running:

```bash
mvn clean install -DskipTests
```

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Flink binary (`install`).

To speed up the build you can skip tests, QA plugins, and JavaDocs:

```bash
mvn clean install -DskipTests -Dfast
```

## Build PyFlink

#### Prerequisites

1. Building Flink

    If you want to build a PyFlink package that can be used for pip installation, you need to build the Flink project first, as described in [Build Flink](#build-flink).

2. Python version(3.6, 3.7 or 3.8) is required

    ```shell
    $ python --version
    # the version printed here must be 3.6, 3.7 or 3.8
    ```

3. Build PyFlink with Cython extension support (optional)

    To build PyFlink with Cython extension support, you’ll need a C compiler. It's a little different on how to install the C compiler on different operating systems:

    * **Linux** Linux operating systems usually come with GCC pre-installed. Otherwise, you need to install it manually. For example, you can install it with command `sudo apt-get install build-essential` On Ubuntu or Debian.

    * **Mac OS X** To install GCC on Mac OS X, you need to download and install "[Command Line Tools for Xcode](https://developer.apple.com/downloads/index.action)", which is available in Apple’s developer page.

    You also need to install the dependencies with following command:

    ```shell
    $ python -m pip install -r flink-python/dev/dev-requirements.txt
    ```

#### Installation

Then go to the root directory of flink source code and run this command to build the sdist package and wheel package of `apache-flink` and `apache-flink-libraries`:

```bash
cd flink-python; python setup.py sdist bdist_wheel; cd apache-flink-libraries; python setup.py sdist; cd ..;
```

The sdist package of `apache-flink-libraries` will be found under `./flink-python/apache-flink-libraries/dist/`. It could be installed as following:

```bash
python -m pip install apache-flink-libraries/dist/*.tar.gz
```

The sdist and wheel packages of `apache-flink` will be found under `./flink-python/dist/`. Either of them could be used for installation, such as:

```bash
python -m pip install dist/*.whl
```

## Dependency Shading

Flink [shades away](https://maven.apache.org/plugins/maven-shade-plugin/) some of the libraries it uses, in order to avoid version clashes with user programs that use different versions of these libraries. Among the shaded libraries are *Google Guava*, *Asm*, *Apache Curator*, *Apache HTTP Components*, *Netty*, and others.

The dependency shading mechanism was recently changed in Maven and requires users to build Flink slightly differently, depending on their Maven version:

**Maven and 3.2.x**
It is sufficient to call `mvn clean install -DskipTests` in the root directory of Flink code base.

**Maven 3.3.x**
The build has to be done in two steps: First in the base directory, then in shaded modules, such as the distribution and the filesystems:

```bash
# build overall project
mvn clean install -DskipTests

# build shaded modules used in dist again, for example:
cd flink-filesystems/flink-s3-fs-presto/
mvn clean install -DskipTests
# ... and other modules

# build dist again to include shaded modules
cd flink-dist
mvn clean install
```

*Note:* To check your Maven version, run `mvn --version`.

*Note:* We recommend using the latest Maven 3.2.x version for building production-grade Flink distributions, as this is the version the Flink developers are using for the official releases and testing.

{{< top >}}


## Scala Versions

{{< hint info >}}
Users that purely use the Java APIs and libraries can *ignore* this section.
{{< /hint >}}

Flink has APIs, libraries, and runtime modules written in [Scala](http://scala-lang.org). Users of the Scala API and libraries may have to match the Scala version of Flink with the Scala version of their projects (because Scala is not strictly backwards compatible).

Since version 1.7 Flink builds with Scala version 2.11 (default) and 2.12.

To build Flink against Scala 2.12, issue the following command:
```bash
mvn clean install -DskipTests -Dscala-2.12
```

To build against a specific binary Scala version you can use:
```bash
mvn clean install -DskipTests -Dscala-2.12 -Dscala.version=<scala version>
```


{{< top >}}

## Encrypted File Systems

If your home directory is encrypted you might encounter a `java.io.IOException: File name too long` exception. Some encrypted file systems, like encfs used by Ubuntu, do not allow long filenames, which is the cause of this error.

The workaround is to add:

```xml
<args>
    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>
</args>
```

in the compiler configuration of the `pom.xml` file of the module causing the error. For example, if the error appears in the `flink-yarn` module, the above code should be added under the `<configuration>` tag of `scala-maven-plugin`. See [this issue](https://issues.apache.org/jira/browse/FLINK-2003) for more information.

{{< top >}}

