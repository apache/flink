---
title: "Quickstart: Scala API"
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

Start working on your Flink Scala program in a few simple steps.

## Requirements

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 6.x__ (or higher) installations.


## Create Project

Use one of the following commands to __create a project__:

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#quickstart-script" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
    <li><a href="#maven-archetype" data-toggle="tab">Use <strong>Maven archetypes</strong></a></li>
</ul>
<div class="tab-content">
    <div class="tab-pane active" id="quickstart-script">
{% highlight bash %}
$ curl http://flink.apache.org/q/quickstart-scala.sh | bash
{% endhighlight %}
    </div>
    <div class="tab-pane" id="maven-archetype">
{% highlight bash %}
$ mvn archetype:generate                             \
  -DarchetypeGroupId=org.apache.flink              \
  -DarchetypeArtifactId=flink-quickstart-scala           \
  -DarchetypeVersion={{site.version}}
{% endhighlight %}
    This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
    </div>
</div>


## Inspect Project

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


## Build Project

If you want to __build your project__, go to your project directory and issue the `mvn clean package -Pbuild-jar` command. You will __find a jar__ that runs on every Flink cluster in __target/your-artifact-id-1.0-SNAPSHOT.jar__. There is also a fat-jar,  __target/your-artifact-id-1.0-SNAPSHOT-flink-fat-jar.jar__. This
also contains all dependencies that get added to the maven project.

## Next Steps

Write your application!

The quickstart project contains a WordCount implementation, the "Hello World" of Big Data processing systems. The goal of WordCount is to determine the frequencies of words in a text, e.g., how often do the terms "the" or "house" occurs in all Wikipedia texts.

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

The following code shows the WordCount implementation from the Quickstart which processes some text lines with two operators (FlatMap and Reduce), and writes the prints the resulting words and counts to std-out.

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

    // execute program
    env.execute("WordCount Example")
  }
}
~~~

{% gh_link /flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/wordcount/WordCount.scala "Check GitHub" %} for the full example code.

For a complete overview over our API, have a look at the [Programming Guide]({{ site.baseurl }}/apis/programming_guide.html) and [further example programs]({{ site.baseurl }}/apis/examples.html). If you have any trouble, ask on our [Mailing List](http://mail-archives.apache.org/mod_mbox/flink-dev/). We are happy to provide help.


