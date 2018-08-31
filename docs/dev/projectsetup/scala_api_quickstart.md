---
title: "Project Template for Scala"
nav-title: Project Template for Scala
nav-parent_id: projectsetup
nav-pos: 1
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
In order to get quickly started, Flink provides project templates for the following build tools:

- [SBT](#sbt)
- [Maven](#maven)

These templates help you to set up the project structure and to create the initial build files.

## SBT

### Create Project

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

### Build Project

In order to build your project you simply have to issue the `sbt clean assembly` command.
This will create the fat-jar __your-project-name-assembly-0.1-SNAPSHOT.jar__ in the directory __target/scala_your-major-scala-version/__.

### Run Project

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
      -DarchetypeArtifactId=flink-quickstart-scala     \{% unless site.is_stable %}
      -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
      -DarchetypeVersion={{site.version}}
    {% endhighlight %}
    This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
    </div>
    <div class="tab-pane" id="quickstart-script">
{% highlight bash %}
{% if site.is_stable %}
    $ curl https://flink.apache.org/q/quickstart-scala.sh | bash -s {{site.version}}
{% else %}
    $ curl https://flink.apache.org/q/quickstart-scala-SNAPSHOT.sh | bash -s {{site.version}}
{% endif %}
{% endhighlight %}
    </div>
    {% unless site.is_stable %}
    <p style="border-radius: 5px; padding: 5px" class="bg-danger">
        <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the commandline. If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven official document</a>
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
        ├── resources
        │   └── log4j.properties
        └── scala
            └── org
                └── myorg
                    └── quickstart
                        ├── BatchJob.scala
                        └── StreamingJob.scala
{% endhighlight %}

The sample project is a __Maven project__, which contains two classes: _StreamingJob_ and _BatchJob_ are the basic skeleton programs for a *DataStream* and *DataSet* program.
The _main_ method is the entry point of the program, both for in-IDE testing/execution and for proper deployments.

We recommend you __import this project into your IDE__.

IntelliJ IDEA supports Maven out of the box and offers a plugin for Scala development.
From our experience, IntelliJ provides the best experience for developing Flink applications.

For Eclipse, you need the following plugins, which you can install from the provided Eclipse Update Sites:

* _Eclipse 4.x_
  * [Scala IDE](http://download.scala-ide.org/sdk/lithium/e44/scala211/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repo1.maven.org/maven2/.m2e/connectors/m2eclipse-buildhelper/0.15.0/N/0.15.0.201207090124/)
* _Eclipse 3.8_
  * [Scala IDE for Scala 2.11](http://download.scala-ide.org/sdk/helium/e38/scala211/stable/site) or [Scala IDE for Scala 2.10](http://download.scala-ide.org/sdk/helium/e38/scala210/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/)

### Build Project

If you want to __build/package your project__, go to your project directory and
run the '`mvn clean package`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `target/<artifact-id>-<version>.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly. That way, the Flink
can run time application from the JAR file without additionally specifying the main class.


## Next Steps

Write your application!

If you are writing a streaming application and you are looking for inspiration what to write,
take a look at the [Stream Processing Application Tutorial]({{ site.baseurl }}/tutorials/datastream_api.html#writing-a-flink-program)

If you are writing a batch processing application and you are looking for inspiration what to write,
take a look at the [Batch Application Examples]({{ site.baseurl }}/dev/batch/examples.html)

For a complete overview over the APIa, have a look at the
[DataStream API]({{ site.baseurl }}/dev/datastream_api.html) and
[DataSet API]({{ site.baseurl }}/dev/batch/index.html) sections.

[Here]({{ site.baseurl }}/tutorials/local_setup.html) you can find out how to run an application outside the IDE on a local cluster.

If you have any trouble, ask on our
[Mailing List](http://mail-archives.apache.org/mod_mbox/flink-user/).
We are happy to provide help.

{% top %}
