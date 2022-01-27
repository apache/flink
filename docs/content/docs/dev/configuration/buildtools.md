---
title: "Using Build Tools"
weight: 2
type: docs
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

# Using build tools to configure your Flink project

You will likely need a build tool to configure your Flink project. This guide will show you how to 
do so with [Maven](https://maven.apache.org), [Gradle](https://gradle.org), and [sbt](https://www.scala-sbt.org). 

{{< tabs "build tool" >}}
{{< tab "Maven" >}}
Maven is an open-source build automation tool developed by the Apache Group that enables you to build, 
publish, and deploy projects. You can use it to manage the entire lifecycle of your software project.
{{< /tab >}}
{{< tab "Gradle" >}}
Gradle is an open-source general-purpose build tool that can be used to automate tasks in the
development process.
{{< /tab >}}
{{< tab "sbt" >}}
sbt is an open-source build tool for managing Scala and Java projects. You can use it to
build, compile, test, as well as manage libraries and dependencies.
{{< /tab >}}
{{< /tabs >}}

### Requirements

{{< tabs "requirements" >}}
{{< tab "Maven" >}}
- Maven 3.0.4 (or higher)
- Java 8.x
{{< /tab >}}
{{< tab "Gradle" >}}
- Gradle 3.x (or higher)
- Java 8.x
{{< /tab >}}
{{< tab "sbt" >}}
- sbt 0.13.13 (or higher)
- Java 8.x
{{< /tab >}}
{{< /tabs >}}

### Creating a Flink project

{{< tabs "creating project" >}}
{{< tab "Maven" >}}

You can create a project based on an [Archetype](https://maven.apache.org/guides/introduction/introduction-to-archetypes.html) 
with the Maven command below or use the provided quickstart bash script.

#### Maven command
```bash
$ mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion={{< version >}}
```
This allows you to name your newly created project and will interactively ask you for the groupId,
artifactId, and package name.

#### Quickstart script
```bash
$ curl https://flink.apache.org/q/quickstart.sh | bash -s {{< version >}}
```

{{< hint info >}}
For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via
the command line. For details about this change, please refer to the <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">official Maven document</a> If you wish to use a snapshot repository, you need to add a
repository entry to your `settings.xml` file. For example:

```xml
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
```
{{< /hint >}}

{{< /tab >}}
{{< tab "Gradle" >}}
You can create a project with a Gradle build script or use the provided quickstart bash script.

#### Gradle build script

To execute these build configuration scripts, run the `gradle` command in the directory with these scripts.

**build.gradle**

```gradle
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
    flinkVersion = '{{< version >}}'
    scalaBinaryVersion = '{{< scala_version >}}'
    slf4jVersion = '1.7.32'
    log4jVersion = '2.17.1'
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
    compile "org.apache.flink:flink-streaming-java:${flinkVersion}"
    compile "org.apache.flink:flink-clients:${flinkVersion}"
    // --------------------------------------------------------------
    // Dependencies that should be part of the shadow jar, e.g.
    // connectors. These must be in the flinkShadowJar configuration!
    // --------------------------------------------------------------
    //flinkShadowJar "org.apache.flink:flink-connector-kafka:${flinkVersion}"
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
```

**settings.gradle**
```gradle
rootProject.name = 'quickstart'
```
#### Quickstart script
```bash
bash -c "$(curl https://flink.apache.org/q/gradle-quickstart.sh)" -- {{< version >}} {{< scala_version >}}
```
{{< /tab >}}
{{< tab "sbt" >}}
You can scaffold a new Flink project with the following [giter8 template](https://github.com/tillrohrmann/flink-project.g8)
and the `sbt new` command (which creates new build definitions from a template) or use the provided quickstart bash script.

#### sbt template
```bash
$ sbt new tillrohrmann/flink-project.g8
```

#### Quickstart script
```bash
$ bash <(curl https://flink.apache.org/q/sbt-quickstart.sh)
```
{{< /tab >}}
{{< /tabs >}}

### Importing the project into your IDE

Once the project folder and files have been created, we recommend that you import this project into
your IDE for developing and testing.

{{< tabs "importing project" >}}
{{< tab "Maven" >}}
IntelliJ IDEA supports Maven projects out-of-the-box.

Eclipse offers the [m2e plugin](http://www.eclipse.org/m2e/) 
to [import Maven projects](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import).
{{< /tab >}}
{{< tab "Gradle" >}}
IntelliJ IDEA supports Gradle projects via the `Gradle` plugin. 

Eclipse does so via the [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship) 
plugin (make sure to specify a Gradle version >= 3.0 in the last step of the import wizard; the `shadow` 
plugin requires it). You may also use [Gradle's IDE integration](https://docs.gradle.org/current/userguide/userguide.html#ide-integration)
to create project files with Gradle.
{{< /tab >}}
{{< tab "sbt" >}}
In IntelliJ IDEA, go to `File -> New -> Project from Existing Sources...` and then choose your project's directory. 
IntelliJ will then automatically detect the `build.sbt` file and set everything up.

To run your Flink job, choose the `mainRunner` module as the classpath in your __Run/Debug Configuration__
via `Run -> Edit Configurations...` and choosing `mainRunner` from the _Use classpath of module_ drop box.
This will ensure that all dependencies which are set to _provided_ will be available upon execution.

In Eclipse, you first have to create Eclipse project files for it which can be done via the
[sbteclipse](https://github.com/typesafehub/sbteclipse) plugin. 

Then add the following line to your `PROJECT_DIR/project/plugins.sbt` file:

```bash
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
```

Then in `sbt`, use the following command to create the Eclipse project files:

```bash
> eclipse
```

Now you can import the project into Eclipse via `File -> Import... -> Existing Projects into Workspace`
and then select the project directory.
{{< /tab >}}
{{< /tabs >}}

*Note*: The default JVM heap size for Java may be too small for Flink and you have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu.
See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

### Building the project

{{< tabs "building project" >}}
{{< tab "Maven" >}}
If you want to build/package your project, navigate to your project directory and run the
'`mvn clean package`' command. You will find a JAR file that contains your application (plus connectors
and libraries that you may have added as dependencies to the application) here:`target/<artifact-id>-<version>.jar`.

__Note:__ If you used a different class than `DataStreamJob` as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly so that Flink
can run the application from the JAR file without additionally specifying the main class.
{{< /tab >}}
{{< tab "Gradle" >}}
If you want to __build/package your project__, go to your project directory and
run the '`gradle clean shadowJar`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `build/libs/<project-name>-<version>-all.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClassName` setting in the `build.gradle` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.
{{< /tab >}}
{{< tab "sbt" >}}
To build your project, issue the `sbt clean assembly` command in the project directory. This will
create the fat-jar `your-project-name-assembly-0.1-SNAPSHOT.jar` in the directory `target/scala_your-major-scala-version/`.

To run your project, issue the `sbt run` command. By default, this will run your job in the same JVM
that `sbt` is running in. In order to run your job in a distinct JVM, add the following line to the
`build.sbt` file:

```scala
fork in run := true
```
{{< /tab >}}
{{< /tabs >}}

### Adding dependencies to the project

You can use [Maven](https://maven.apache.org), [Gradle](https://gradle.org/), or [sbt](https://www.scala-sbt.org/)
to configure your project and add these dependencies.


As an example, you can add the Kafka connector as a dependency like this (in Maven syntax):

{{< artifact flink-connector-kafka >}}


**Important:** Note that all these dependencies should have their scope set to [*provided*](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#dependency-scope). This means that
they are needed to compile against, but that they should not be packaged into the project's resulting
application JAR file. If not set to *provided*, the best case scenario is that the resulting JAR
becomes excessively large, because it also contains all Flink core dependencies. The worst case scenario
is that the Flink core dependencies that are added to the application's JAR file clash with some of
your own dependency versions (which is normally avoided through inverted classloading).


Projects created from the `Java Project Template`, the `Scala Project Template`, or Gradle are configured
to automatically include the application dependencies into the application JAR when you run `mvn clean package`.
For projects that are not set up from those templates, we recommend adding the Maven Shade Plugin to
build the application jar with all required dependencies.

**Important:** For Maven (and other build tools) to correctly package the dependencies into the application
jar, these application dependencies must be specified in scope *compile* (unlike the core dependencies,
which must be specified in scope *provided*).


### Shading


In order to create an uber JAR to run the job, do this:

[ FILL IN ]

**Note:** You do not need to shade Flink API dependencies. You only need to do this for connectors,
formats and third-party dependencies.


# Template for building a JAR with Dependencies

To build an application JAR that contains all dependencies required for declared connectors and libraries,
you can use the following shade plugin definition:

```xml
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
```
