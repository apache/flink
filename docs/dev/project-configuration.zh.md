---
title: "Project Configuration"
nav-parent_id: streaming
nav-pos: 301
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

Every Flink application depends on a set of Flink libraries. At the bare minimum, the application depends
on the Flink APIs. Many applications depend in addition on certain connector libraries (like Kafka, Cassandra, etc.).
When running Flink applications (either in a distributed deployment, or in the IDE for testing), the Flink
runtime library must be available as well.

* This will be replaced by the TOC
{:toc}

## Flink Core and Application Dependencies

As with most systems that run user-defined applications, there are two broad categories of dependencies and libraries in Flink:

  - **Flink Core Dependencies**: Flink itself consists of a set of classes and dependencies that are needed to run the system, for example
    coordination, networking, checkpoints, failover, APIs, operations (such as windowing), resource management, etc.
    The set of all these classes and dependencies forms the core of Flink's runtime and must be present when a Flink
    application is started.

    These core classes and dependencies are packaged in the `flink-dist` jar. They are part of Flink's `lib` folder and
    part of the basic Flink container images. Think of these dependencies as similar to Java's core library (`rt.jar`, `charsets.jar`, etc.),
    which contains the classes like `String` and `List`.

    The Flink Core Dependencies do not contain any connectors or libraries (CEP, SQL, ML, etc.) in order to avoid having an excessive
    number of dependencies and classes in the classpath by default. In fact, we try to keep the core dependencies as slim as possible
    to keep the default classpath small and avoid dependency clashes.

  - The **User Application Dependencies** are all connectors, formats, or libraries that a specific user application needs.

    The user application is typically packaged into an *application jar*, which contains the application code and the required
    connector and library dependencies.

    The user application dependencies explicitly do not include the Flink DataStream APIs and runtime dependencies,
    because those are already part of Flink's Core Dependencies.


## Setting up a Project: Basic Dependencies

Every Flink application needs as the bare minimum the API dependencies, to develop against.

When setting up a project manually, you need to add the following dependencies for the Java/Scala API
(here presented in Maven syntax, but the same dependencies apply to other build tools (Gradle, SBT, etc.) as well.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
</div>

**Important:** Please note that all these dependencies have their scope set to *provided*.
That means that they are needed to compile against, but that they should not be packaged into the
project's resulting application jar file - these dependencies are Flink Core Dependencies,
which are already available in any setup.

It is highly recommended keeping the dependencies in scope *provided*. If they are not set to *provided*,
the best case is that the resulting JAR becomes excessively large, because it also contains all Flink core
dependencies. The worst case is that the Flink core dependencies that are added to the application's jar file
clash with some of your own dependency versions (which is normally avoided through inverted classloading).

**Note on IntelliJ:** To make the applications run within IntelliJ IDEA it is necessary to tick the
`Include dependencies with "Provided" scope` box in the run configuration.
If this option is not available (possibly due to using an older IntelliJ IDEA version), then a simple workaround
is to create a test that calls the applications `main()` method.


## Adding Connector and Library Dependencies

Most applications need specific connectors or libraries to run, for example a connector to Kafka, Cassandra, etc.
These connectors are not part of Flink's core dependencies and must be added as dependencies to the application.

Below is an example adding the connector for Kafka as a dependency (Maven syntax):
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka{{ site.scala_version_suffix }}</artifactId>
    <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

We recommend packaging the application code and all its required dependencies into one *jar-with-dependencies* which
we refer to as the *application jar*. The application jar can be submitted to an already running Flink cluster,
or added to a Flink application container image.

Projects created from the [Java Project Template]({{ site.baseurl }}/dev/project-configuration.html) or
[Scala Project Template]({{ site.baseurl }}/dev/project-configuration) are configured to automatically include
the application dependencies into the application jar when running `mvn clean package`. For projects that are
not set up from those templates, we recommend adding the Maven Shade Plugin (as listed in the Appendix below)
to build the application jar with all required dependencies.

**Important:** For Maven (and other build tools) to correctly package the dependencies into the application jar,
these application dependencies must be specified in scope *compile* (unlike the core dependencies, which
must be specified in scope *provided*).


## Scala Versions

Scala versions (2.11, 2.12, etc.) are not binary compatible with one another.
For that reason, Flink for Scala 2.11 cannot be used with an application that uses
Scala 2.12.

All Flink dependencies that (transitively) depend on Scala are suffixed with the
Scala version that they are built for, for example `flink-streaming-scala_2.11`.

Developers that only use Java can pick any Scala version, Scala developers need to
pick the Scala version that matches their application's Scala version.

Please refer to the [build guide]({{ site.baseurl }}/flinkDev/building.html#scala-versions)
for details on how to build Flink for a specific Scala version.

## Hadoop Dependencies

**General rule: It should never be necessary to add Hadoop dependencies directly to your application.**
*(The only exception being when using existing Hadoop input-/output formats with Flink's Hadoop compatibility wrappers)*

If you want to use Flink with Hadoop, you need to have a Flink setup that includes the Hadoop dependencies, rather than
adding Hadoop as an application dependency. Please refer to the [Hadoop Setup Guide]({{ site.baseurl }}/ops/deployment/hadoop.html)
for details.

There are two main reasons for that design:

  - Some Hadoop interaction happens in Flink's core, possibly before the user application is started, for example
    setting up HDFS for checkpoints, authenticating via Hadoop's Kerberos tokens, or deployment on YARN.

  - Flink's inverted classloading approach hides many transitive dependencies from the core dependencies. That applies not only
    to Flink's own core dependencies, but also to Hadoop's dependencies when present in the setup.
    That way, applications can use different versions of the same dependencies without running into dependency conflicts (and
    trust us, that's a big deal, because Hadoops dependency tree is huge.)

If you need Hadoop dependencies during testing or development inside the IDE (for example for HDFS access), please configure
these dependencies similar to the scope of the dependencies to *test* or to *provided*.

## Maven Quickstart

#### Requirements

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 8.x__ installations.

#### Create Project

Use one of the following commands to __create a project__:

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#maven-archetype" data-toggle="tab">Use <strong>Maven archetypes</strong></a></li>
    <li><a href="#quickstart-script" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
</ul>
<div class="tab-content">
    <div class="tab-pane active" id="maven-archetype">
{% highlight bash %}
$ mvn archetype:generate                               \
  -DarchetypeGroupId=org.apache.flink              \
  -DarchetypeArtifactId=flink-quickstart-java      \{% unless site.is_stable %}
  -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
  -DarchetypeVersion={{site.version}}
{% endhighlight %}
        This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
    </div>
    <div class="tab-pane" id="quickstart-script">
{% highlight bash %}
{% if site.is_stable %}
$ curl https://flink.apache.org/q/quickstart.sh | bash -s {{site.version}}
{% else %}
$ curl https://flink.apache.org/q/quickstart-SNAPSHOT.sh | bash -s {{site.version}}
{% endif %}
{% endhighlight %}

    </div>
    {% unless site.is_stable %}
    <p style="border-radius: 5px; padding: 5px" class="bg-danger">
        <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the command line. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven official document</a>
        If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For example:
{% highlight bash %}
<settings>
  <activeProfiles>
    <activeProfile>apache</activeProfile>
  </activeProfiles>
  <profiles>
    <profile>
      <id>apache</id>
      <repositories>
        <repository>
          <id>apache-snapshots</id>
          <url>https://repository.apache.org/content/repositories/snapshots/</url>
        </repository>
      </repositories>
    </profile>
  </profiles>
</settings>
{% endhighlight %}
    </p>
    {% endunless %}
</div>

We recommend you __import this project into your IDE__ to develop and
test it. IntelliJ IDEA supports Maven projects out of the box.
If you use Eclipse, the [m2e plugin](http://www.eclipse.org/m2e/)
allows to [import Maven projects](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import).
Some Eclipse bundles include that plugin by default, others require you
to install it manually. 

*Please note*: The default JVM heapsize for Java may be too
small for Flink. You have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu. See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details. 

#### Build Project

If you want to __build/package your project__, go to your project directory and
run the '`mvn clean package`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `target/<artifact-id>-<version>.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.

## Gradle

#### Requirements

The only requirements are working __Gradle 3.x__ (or higher) and __Java 8.x__ installations.

#### Create Project

Use one of the following commands to __create a project__:

<ul class="nav nav-tabs" style="border-bottom: none;">
        <li class="active"><a href="#gradle-example" data-toggle="tab"><strong>Gradle example</strong></a></li>
    <li><a href="#gradle-script" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
</ul>
<div class="tab-content">
    <div class="tab-pane active" id="gradle-example">

        <ul class="nav nav-tabs" style="border-bottom: none;">
            <li class="active"><a href="#gradle-build" data-toggle="tab"><tt>build.gradle</tt></a></li>
            <li><a href="#gradle-settings" data-toggle="tab"><tt>settings.gradle</tt></a></li>
        </ul>
        <div class="tab-content">
<!-- NOTE: Any change to the build scripts here should also be reflected in flink-web/q/gradle-quickstart.sh !! -->
            <div class="tab-pane active" id="gradle-build">
{% highlight gradle %}
buildscript {
    repositories {
        jcenter() // this applies only to the Gradle 'Shadow' plugin
    }
    dependencies {
        classpath 'com.github.jengelman.gradle.plugins:shadow:2.0.4'
    }
}

plugins {
    id 'java'
    id 'application'
    // shadow plugin to produce fat JARs
    id 'com.github.johnrengelman.shadow' version '2.0.4'
}


// artifact properties
group = 'org.myorg.quickstart'
version = '0.1-SNAPSHOT'
mainClassName = 'org.myorg.quickstart.StreamingJob'
description = """Flink Quickstart Job"""

ext {
    javaVersion = '1.8'
    flinkVersion = '{{ site.version }}'
    scalaBinaryVersion = '{{ site.scala_version }}'
    slf4jVersion = '1.7.15'
    log4jVersion = '2.12.1'
}


sourceCompatibility = javaVersion
targetCompatibility = javaVersion
tasks.withType(JavaCompile) {
	options.encoding = 'UTF-8'
}

applicationDefaultJvmArgs = ["-Dlog4j.configurationFile=log4j2.properties"]

task wrapper(type: Wrapper) {
    gradleVersion = '3.1'
}

// declare where to find the dependencies of your project
repositories {
    mavenCentral()
    maven { url "https://repository.apache.org/content/repositories/snapshots/" }
}

// NOTE: We cannot use "compileOnly" or "shadow" configurations since then we could not run code
// in the IDE or with "gradle run". We also cannot exclude transitive dependencies from the
// shadowJar yet (see https://github.com/johnrengelman/shadow/issues/159).
// -> Explicitly define the // libraries we want to be included in the "flinkShadowJar" configuration!
configurations {
    flinkShadowJar // dependencies which go into the shadowJar

    // always exclude these (also from transitive dependencies) since they are provided by Flink
    flinkShadowJar.exclude group: 'org.apache.flink', module: 'force-shading'
    flinkShadowJar.exclude group: 'com.google.code.findbugs', module: 'jsr305'
    flinkShadowJar.exclude group: 'org.slf4j'
    flinkShadowJar.exclude group: 'org.apache.logging.log4j'
}

// declare the dependencies for your production and test code
dependencies {
    // --------------------------------------------------------------
    // Compile-time dependencies that should NOT be part of the
    // shadow jar and are provided in the lib folder of Flink
    // --------------------------------------------------------------
    compile "org.apache.flink:flink-streaming-java_${scalaBinaryVersion}:${flinkVersion}"

    // --------------------------------------------------------------
    // Dependencies that should be part of the shadow jar, e.g.
    // connectors. These must be in the flinkShadowJar configuration!
    // --------------------------------------------------------------
    //flinkShadowJar "org.apache.flink:flink-connector-kafka_${scalaBinaryVersion}:${flinkVersion}"

    compile "org.apache.logging.log4j:log4j-api:${log4jVersion}"
    compile "org.apache.logging.log4j:log4j-core:${log4jVersion}"
    compile "org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}"
    compile "org.slf4j:slf4j-log4j12:${slf4jVersion}"

    // Add test dependencies here.
    // testCompile "junit:junit:4.12"
}

// make compileOnly dependencies available for tests:
sourceSets {
    main.compileClasspath += configurations.flinkShadowJar
    main.runtimeClasspath += configurations.flinkShadowJar

    test.compileClasspath += configurations.flinkShadowJar
    test.runtimeClasspath += configurations.flinkShadowJar

    javadoc.classpath += configurations.flinkShadowJar
}

run.classpath = sourceSets.main.runtimeClasspath

jar {
    manifest {
        attributes 'Built-By': System.getProperty('user.name'),
                'Build-Jdk': System.getProperty('java.version')
    }
}

shadowJar {
    configurations = [project.configurations.flinkShadowJar]
}
{% endhighlight %}
            </div>
            <div class="tab-pane" id="gradle-settings">
{% highlight gradle %}
rootProject.name = 'quickstart'
{% endhighlight %}
            </div>
        </div>
    </div>

    <div class="tab-pane" id="gradle-script">
{% highlight bash %}
bash -c "$(curl https://flink.apache.org/q/gradle-quickstart.sh)" -- {{site.version}} {{site.scala_version}}
{% endhighlight %}
    This allows you to <strong>name your newly created project</strong>. It will interactively ask
    you for the project name, organization (also used for the package name), project version,
    Scala and Flink version.
    </div>
</div>

We recommend you __import this project into your IDE__ to develop and
test it. IntelliJ IDEA supports Gradle projects after installing the `Gradle` plugin.
Eclipse does so via the [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship) plugin
(make sure to specify a Gradle version >= 3.0 in the last step of the import wizard; the `shadow` plugin requires it).
You may also use [Gradle's IDE integration](https://docs.gradle.org/current/userguide/userguide.html#ide-integration)
to create project files from Gradle.


*Please note*: The default JVM heapsize for Java may be too
small for Flink. You have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu. See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

#### Build Project

If you want to __build/package your project__, go to your project directory and
run the '`gradle clean shadowJar`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `build/libs/<project-name>-<version>-all.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClassName` setting in the `build.gradle` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.

## SBT

#### Create Project

You can scaffold a new project via either of the following two methods:

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#sbt_template" data-toggle="tab">Use the <strong>sbt template</strong></a></li>
    <li><a href="#quickstart-script-sbt" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
</ul>

<div class="tab-content">
    <div class="tab-pane active" id="sbt_template">
{% highlight bash %}
$ sbt new tillrohrmann/flink-project.g8
{% endhighlight %}
    This will prompt you for a couple of parameters (project name, flink version...) and then create a Flink project from the <a href="https://github.com/tillrohrmann/flink-project.g8">flink-project template</a>.
    You need sbt >= 0.13.13 to execute this command. You can follow this <a href="http://www.scala-sbt.org/download.html">installation guide</a> to obtain it if necessary.
    </div>
    <div class="tab-pane" id="quickstart-script-sbt">
{% highlight bash %}
$ bash <(curl https://flink.apache.org/q/sbt-quickstart.sh)
{% endhighlight %}
    This will create a Flink project in the <strong>specified</strong> project directory.
    </div>
</div>

#### Build Project

In order to build your project you simply have to issue the `sbt clean assembly` command.
This will create the fat-jar __your-project-name-assembly-0.1-SNAPSHOT.jar__ in the directory __target/scala_your-major-scala-version/__.

#### Run Project

In order to run your project you have to issue the `sbt run` command.

Per default, this will run your job in the same JVM as `sbt` is running.
In order to run your job in a distinct JVM, add the following line to `build.sbt`

{% highlight scala %}
fork in run := true
{% endhighlight %}


#### IntelliJ

We recommend using [IntelliJ](https://www.jetbrains.com/idea/) for your Flink job development.
In order to get started, you have to import your newly created project into IntelliJ.
You can do this via `File -> New -> Project from Existing Sources...` and then choosing your project's directory.
IntelliJ will then automatically detect the `build.sbt` file and set everything up.

In order to run your Flink job, it is recommended to choose the `mainRunner` module as the classpath of your __Run/Debug Configuration__.
This will ensure, that all dependencies which are set to _provided_ will be available upon execution.
You can configure the __Run/Debug Configurations__ via `Run -> Edit Configurations...` and then choose `mainRunner` from the _Use classpath of module_ dropbox.

#### Eclipse

In order to import the newly created project into [Eclipse](https://eclipse.org/), you first have to create Eclipse project files for it.
These project files can be created via the [sbteclipse](https://github.com/typesafehub/sbteclipse) plugin.
Add the following line to your `PROJECT_DIR/project/plugins.sbt` file:

{% highlight bash %}
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
{% endhighlight %}

In `sbt` use the following command to create the Eclipse project files

{% highlight bash %}
> eclipse
{% endhighlight %}

Now you can import the project into Eclipse via `File -> Import... -> Existing Projects into Workspace` and then select the project directory.


## Appendix: Template for building a Jar with Dependencies

To build an application JAR that contains all dependencies required for declared connectors and libraries,
you can use the following shade plugin definition:

{% highlight xml %}
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.1.1</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>com.google.code.findbugs:jsr305</exclude>
                                <exclude>org.slf4j:*</exclude>
                                <exclude>log4j:*</exclude>
                            </excludes>
                        </artifactSet>
                        <filters>
                            <filter>
                                <!-- Do not copy the signatures in the META-INF folder.
                                Otherwise, this might cause SecurityExceptions when using the JAR. -->
                                <artifact>*:*</artifact>
                                <excludes>
                                    <exclude>META-INF/*.SF</exclude>
                                    <exclude>META-INF/*.DSA</exclude>
                                    <exclude>META-INF/*.RSA</exclude>
                                </excludes>
                            </filter>
                        </filters>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>my.programs.main.clazz</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
{% endhighlight %}

{% top %}
