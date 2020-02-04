---
title: 从源码构建 Flink
nav-parent_id: flinkdev
nav-pos: 20
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

This page covers how to build Flink {{ site.version }} from sources.

* This will be replaced by the TOC
{:toc}

## Build Flink

In order to build Flink you need the source code. Either [download the source of a release]({{ site.download_url }}) or [clone the git repository]({{ site.github_url }}).

In addition you need **Maven 3** and a **JDK** (Java Development Kit). Flink requires **at least Java 8** to build.

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.2.5 creates the libraries properly.
To build unit tests use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

To clone from git, enter:

{% highlight bash %}
git clone {{ site.github_url }}
{% endhighlight %}

The simplest way of building Flink is by running:

{% highlight bash %}
mvn clean install -DskipTests
{% endhighlight %}

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Flink binary (`install`).

To speed up the build you can skip tests, QA plugins, and JavaDocs:

{% highlight bash %}
mvn clean install -DskipTests -Dfast
{% endhighlight %}

## 构建PyFlink

如果您想构建一个可用于pip安装的PyFlink包，您需要先构建Flink的Jar包，如[构建Flink](#build-flink)中所述。
之后，进入Flink源码根目录，并执行以下命令，构建PyFlink的源码发布包和wheel包：

{% highlight bash %}
# 注: 执行以下命令设置版本（临时性需要）
VERSION=`grep "^version:" docs/_config.yml | awk -F'\"' '{print $2}'`
cd flink-python; perl -pi -e "s#^__version__ = \".*\"#__version__ = \"${VERSION}\"#" pyflink/version.py
# 打包
python3 setup.py sdist bdist_wheel
{% endhighlight %}

构建好的源码发布包和wheel包位于`./flink-python/dist/`目录下。它们均可使用pip安装,比如:

{% highlight bash %}
pip install dist/*.tar.gz
{% endhighlight %}

## Dependency Shading

Flink [shades away](https://maven.apache.org/plugins/maven-shade-plugin/) some of the libraries it uses, in order to avoid version clashes with user programs that use different versions of these libraries. Among the shaded libraries are *Google Guava*, *Asm*, *Apache Curator*, *Apache HTTP Components*, *Netty*, and others.

The dependency shading mechanism was recently changed in Maven and requires users to build Flink slightly differently, depending on their Maven version:

**Maven 3.1.x and 3.2.x**
It is sufficient to call `mvn clean install -DskipTests` in the root directory of Flink code base.

**Maven 3.3.x**
The build has to be done in two steps: First in the base directory, then in the distribution project:

{% highlight bash %}
mvn clean install -DskipTests
cd flink-dist
mvn clean install
{% endhighlight %}

*Note:* To check your Maven version, run `mvn --version`.

{% top %}

## Hadoop Versions

Flink has optional dependencies to HDFS and YARN which are both dependencies from [Apache Hadoop](http://hadoop.apache.org). There exist many different versions of Hadoop (from both the upstream project and the different Hadoop distributions). If you are using an incompatible combination of versions, exceptions may occur.

Flink can be built against any Hadoop version >= 2.4.0, but depending on the version it may be a 1 or 2 step process.

### Pre-bundled versions

To build against Hadoop 2.4.1, 2.6.5, 2.7.5 or 2.8.3, it is sufficient to run (e.g., for version `2.6.5`):

{% highlight bash %}
mvn clean install -DskipTests -Dhadoop.version=2.6.5
{% endhighlight %}

To package a shaded pre-packaged Hadoop jar into the distributions `/lib` directory, activate the `include-hadoop` profile`:

{% highlight bash %}
mvn clean install -DskipTests -Pinclude-hadoop
{% endhighlight %}

### Custom / Vendor-specific versions

If you want to build against Hadoop version that is *NOT* 2.4.1, 2.6.5, 2.7.5 or 2.8.3,
then it is first necessary to build [flink-shaded](https://github.com/apache/flink-shaded) against this version.
You can find the source for this project in the [Additional Components]({{ site.download_url }}#additional-components) section of the download page.

<span class="label label-info">Note</span> If you want to build `flink-shaded` against a vendor specific Hadoop version, you first have to configure the
vendor-specific maven repository in your local maven setup as described [here](https://maven.apache.org/guides/mini/guide-multiple-repositories.html).

Run the following command to build and install `flink-shaded` against your desired Hadoop version (e.g., for version `2.6.5-custom`):

{% highlight bash %}
mvn clean install -Dhadoop.version=2.6.5-custom
{% endhighlight %}

After this step is complete, follow the steps for [Pre-bundled versions](#pre-bundled-versions).

{% top %}

## Scala Versions

{% info %} Users that purely use the Java APIs and libraries can *ignore* this section.

Flink has APIs, libraries, and runtime modules written in [Scala](http://scala-lang.org). Users of the Scala API and libraries may have to match the Scala version of Flink with the Scala version of their projects (because Scala is not strictly backwards compatible).

Since version 1.7 Flink builds with Scala version 2.11 (default) and 2.12.

To build FLink against Scala 2.12, issue the following command:
{% highlight bash %}
mvn clean install -DskipTests -Dscala-2.12
{% endhighlight %}

{% top %}

## Encrypted File Systems

If your home directory is encrypted you might encounter a `java.io.IOException: File name too long` exception. Some encrypted file systems, like encfs used by Ubuntu, do not allow long filenames, which is the cause of this error.

The workaround is to add:

{% highlight xml %}
<args>
    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>
</args>
{% endhighlight %}

in the compiler configuration of the `pom.xml` file of the module causing the error. For example, if the error appears in the `flink-yarn` module, the above code should be added under the `<configuration>` tag of `scala-maven-plugin`. See [this issue](https://issues.apache.org/jira/browse/FLINK-2003) for more information.

## Jackson

Multiple Flink components use [Jackson](https://github.com/FasterXML/jackson). Older versions of jackson (<`2.10.1`) are subject to a variety of security vulnerabilities.

Flink 1.9.2+ offers an opt-in profile (`use-jackson-2.10.1`) for building Flink against Jackson `2.10.1`; including `jackson-annotations`, `jackson-core` and `jackson-databind`.

Usage: `mvn package -Puse-jackson-2.10.1`

When you build a maven application against this Flink version it is recommended to bump the `maven-shade-plugin` version to at least `3.1.1` to prevent packaging errors.

{% top %}

