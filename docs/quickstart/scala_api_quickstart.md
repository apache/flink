---
title: "Quickstart: Scala API"
# Top navigation
top-nav-group: quickstart
top-nav-pos: 4
top-nav-title: Scala API
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

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#giter8" data-toggle="tab">Use <strong>Giter8</strong></a></li>
    <li><a href="#clone-repository" data-toggle="tab">Clone <strong>repository</strong></a></li>
    <li><a href="#quickstart-script-sbt" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
</ul>

<div class="tab-content">
    <div class="tab-pane active" id="giter8">
    {% highlight bash %}
    $ g8 tillrohrmann/flink-project
    {% endhighlight %}
    This will create a Flink project in the <strong>specified</strong> project directory from the <a href="https://github.com/tillrohrmann/flink-project.g8">flink-project template</a>.
    If you haven't installed <a href="https://github.com/n8han/giter8">giter8</a>, then please follow this <a href="https://github.com/n8han/giter8#installation">installation guide</a>. 
    </div>
    <div class="tab-pane" id="clone-repository">
    {% highlight bash %}
    $ git clone https://github.com/tillrohrmann/flink-project.git
    {% endhighlight %}
    This will create the Flink project in the directory <strong>flink-project</strong>. 
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

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 7.x__ (or higher) installations.


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
      -DarchetypeArtifactId=flink-quickstart-scala     \
      -DarchetypeVersion={{site.version}}
    {% endhighlight %}
    This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
    </div>
    <div class="tab-pane" id="quickstart-script">
{% highlight bash %}
$ curl https://flink.apache.org/q/quickstart-scala.sh | bash
{% endhighlight %}
    </div>
</div>


### Inspect Project

There will be a new directory in your working directory. If you've used the _curl_ approach, the directory is called `quickstart`. Otherwise, it has the name of your artifactId.

The sample project is a __Maven project__, which contains two classes. _Job_ is a basic skeleton program and _WordCountJob_ a working example. Please note that the _main_ method of both classes allow you to start Flink in a development/testing mode.

We recommend to __import this project into your IDE__. For Eclipse, you need the following plugins, which you can install from the provided Eclipse Update Sites:

* _Eclipse 4.x_
  * [Scala IDE](http://download.scala-ide.org/sdk/e38/scala210/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.15.0/N/0.15.0.201206251206/)
* _Eclipse 3.7_
  * [Scala IDE](http://download.scala-ide.org/sdk/e37/scala210/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/)

The IntelliJ IDE also supports Maven and offers a plugin for Scala development.


### Build Project

If you want to __build your project__, go to your project directory and issue the `mvn clean package -Pbuild-jar` command. You will __find a jar__ that runs on every Flink cluster in __target/your-artifact-id-1.0-SNAPSHOT.jar__. There is also a fat-jar,  __target/your-artifact-id-1.0-SNAPSHOT-flink-fat-jar.jar__. This
also contains all dependencies that get added to the maven project.

## Next Steps

Write your application!

The quickstart project contains a WordCount implementation, the "Hello World" of Big Data processing systems. The goal of WordCount is to determine the frequencies of words in a text, e.g., how often do the terms "the" or "house" occur in all Wikipedia texts.

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

The following code shows the WordCount implementation from the Quickstart which processes some text lines with two operators (FlatMap and Reduce), and prints the resulting words and counts to std-out.

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

    // emit result
    counts.print()
  }
}
~~~

{% gh_link /flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/wordcount/WordCount.scala "Check GitHub" %} for the full example code.

For a complete overview over our API, have a look at the [Programming Guide]({{ site.baseurl }}/apis/programming_guide.html) and [further example programs]({{ site.baseurl }}/apis/examples.html). If you have any trouble, ask on our [Mailing List](http://mail-archives.apache.org/mod_mbox/flink-dev/). We are happy to provide help.


