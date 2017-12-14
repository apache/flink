---
title: "Sample Project using the Scala API"
nav-title: Sample Project in Scala
nav-parent_id: start
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

~~~scala
fork in run := true
~~~


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

~~~bash
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
~~~

In `sbt` use the following command to create the Eclipse project files

~~~bash
> eclipse
~~~

Now you can import the project into Eclipse via `File -> Import... -> Existing Projects into Workspace` and then select the project directory.

## Maven

### Requirements

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 8.x__ (or higher) installations.


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
    $ curl https://flink.apache.org/q/quickstart-scala.sh | bash
{% else %}
    $ curl https://flink.apache.org/q/quickstart-scala-SNAPSHOT.sh | bash
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
                        ├── SocketTextStreamWordCount.scala
                        ├── StreamingJob.scala
                        └── WordCount.scala
{% endhighlight %}

The sample project is a __Maven project__, which contains four classes. _StreamingJob_ and _BatchJob_ are basic skeleton programs, _SocketTextStreamWordCount_ is a working streaming example and _WordCountJob_ is a working batch example. Please note that the _main_ method of all classes allow you to start Flink in a development/testing mode.

We recommend you __import this project into your IDE__. For Eclipse, you need the following plugins, which you can install from the provided Eclipse Update Sites:

* _Eclipse 4.x_
  * [Scala IDE](http://download.scala-ide.org/sdk/lithium/e44/scala211/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repo1.maven.org/maven2/.m2e/connectors/m2eclipse-buildhelper/0.15.0/N/0.15.0.201207090124/)
* _Eclipse 3.8_
  * [Scala IDE for Scala 2.11](http://download.scala-ide.org/sdk/helium/e38/scala211/stable/site) or [Scala IDE for Scala 2.10](http://download.scala-ide.org/sdk/helium/e38/scala210/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/)

The IntelliJ IDE supports Maven out of the box and offers a plugin for
Scala development.


### Build Project

If you want to __build your project__, go to your project directory and
issue the `mvn clean package -Pbuild-jar` command. You will
__find a jar__ that runs on every Flink cluster with a compatible
version, __target/original-your-artifact-id-your-version.jar__. There
is also a fat-jar in  __target/your-artifact-id-your-version.jar__ which,
additionally, contains all dependencies that were added to the Maven
project.

## Next Steps

Write your application!

The quickstart project contains a `WordCount` implementation, the
"Hello World" of Big Data processing systems. The goal of `WordCount`
is to determine the frequencies of words in a text, e.g., how often do
the terms "the" or "house" occur in all Wikipedia texts.

__Sample Input__:

~~~bash
big data is big
~~~

__Sample Output__:

~~~bash
big 2
data 1
is 1
~~~

The following code shows the `WordCount` implementation from the
Quickstart which processes some text lines with two operators (a FlatMap
and a Reduce operation via aggregating a sum), and prints the resulting
words and counts to std-out.

~~~scala
object WordCountJob {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // emit result and print result
    counts.print()
  }
}
~~~

{% gh_link flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala/wordcount/WordCount.scala "Check GitHub" %} for the full example code.

For a complete overview over our API, have a look at the
[DataStream API]({{ site.baseurl }}/dev/datastream_api.html),
[DataSet API]({{ site.baseurl }}/dev/batch/index.html), and
[Scala API Extensions]({{ site.baseurl }}/dev/scala_api_extensions.html)
sections. If you have any trouble, ask on our
[Mailing List](http://mail-archives.apache.org/mod_mbox/flink-user/).
We are happy to provide help.

{% top %}
