---
title: "Project Template for Java"
nav-title: Project Template for Java
nav-parent_id: projectsetup
nav-pos: 0
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

* This will be replaced by the TOC
{:toc}


## Build Tools

Flink projects can be built with different build tools.
In order to get started quickly, Flink provides project templates for the following build tools:

- [Maven](#maven)
- [Gradle](#gradle)

These templates help you to set up the project structure and to create the initial build files.

## Maven

### Requirements

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 8.x__ installations.

### Create Project

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
        <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the command line. If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven official document</a>
    </p>
    {% endunless %}
</div>

### Inspect Project

There will be a new directory in your working directory. If you've used
the _curl_ approach, the directory is called `quickstart`. Otherwise,
it has the name of your `artifactId`:

{% highlight bash %}
$ tree quickstart/
quickstart/
├── pom.xml
└── src
    └── main
        ├── java
        │   └── org
        │       └── myorg
        │           └── quickstart
        │               ├── BatchJob.java
        │               └── StreamingJob.java
        └── resources
            └── log4j.properties
{% endhighlight %}

The sample project is a __Maven project__, which contains two classes: _StreamingJob_ and _BatchJob_ are the basic skeleton programs for a *DataStream* and *DataSet* program.
The _main_ method is the entry point of the program, both for in-IDE testing/execution and for proper deployments.

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

### Build Project

If you want to __build/package your project__, go to your project directory and
run the '`mvn clean package`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `target/<artifact-id>-<version>.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.

## Gradle

### Requirements

The only requirements are working __Gradle 3.x__ (or higher) and __Java 8.x__ installations.

### Create Project

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
    slf4jVersion = '1.7.7'
    log4jVersion = '1.2.17'
}


sourceCompatibility = javaVersion
targetCompatibility = javaVersion
tasks.withType(JavaCompile) {
	options.encoding = 'UTF-8'
}

applicationDefaultJvmArgs = ["-Dlog4j.configuration=log4j.properties"]

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
    flinkShadowJar.exclude group: 'log4j'
}

// declare the dependencies for your production and test code
dependencies {
    // --------------------------------------------------------------
    // Compile-time dependencies that should NOT be part of the
    // shadow jar and are provided in the lib folder of Flink
    // --------------------------------------------------------------
    compile "org.apache.flink:flink-java:${flinkVersion}"
    compile "org.apache.flink:flink-streaming-java_${scalaBinaryVersion}:${flinkVersion}"

    // --------------------------------------------------------------
    // Dependencies that should be part of the shadow jar, e.g.
    // connectors. These must be in the flinkShadowJar configuration!
    // --------------------------------------------------------------
    //flinkShadowJar "org.apache.flink:flink-connector-kafka-0.11_${scalaBinaryVersion}:${flinkVersion}"

    compile "log4j:log4j:${log4jVersion}"
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

### Inspect Project

There will be a new directory in your working directory based on the
project name you provided, e.g. for `quickstart`:

{% highlight bash %}
$ tree quickstart/
quickstart/
├── README
├── build.gradle
├── settings.gradle
└── src
    └── main
        ├── java
        │   └── org
        │       └── myorg
        │           └── quickstart
        │               ├── BatchJob.java
        │               └── StreamingJob.java
        └── resources
            └── log4j.properties
{% endhighlight %}

The sample project is a __Gradle project__, which contains two classes: _StreamingJob_ and _BatchJob_ are the basic skeleton programs for a *DataStream* and *DataSet* program.
The _main_ method is the entry point of the program, both for in-IDE testing/execution and for proper deployments.

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

### Build Project

If you want to __build/package your project__, go to your project directory and
run the '`gradle clean shadowJar`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `build/libs/<project-name>-<version>-all.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClassName` setting in the `build.gradle` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.

## Next Steps

Write your application!

If you are writing a streaming application and you are looking for inspiration what to write,
take a look at the [Stream Processing Application Tutorial]({{ site.baseurl }}/getting-started/walkthroughs/datastream_api.html).

If you are writing a batch processing application and you are looking for inspiration what to write,
take a look at the [Batch Application Examples]({{ site.baseurl }}/dev/batch/examples.html).

For a complete overview over the APIs, have a look at the
[DataStream API]({{ site.baseurl }}/dev/datastream_api.html) and
[DataSet API]({{ site.baseurl }}/dev/batch/index.html) sections.

[Here]({{ site.baseurl }}/ops/deployment/local.html) you can find out how to run an application outside the IDE on a local cluster.

If you have any trouble, ask on our
[Mailing List](http://mail-archives.apache.org/mod_mbox/flink-user/).
We are happy to provide help.

{% top %}
