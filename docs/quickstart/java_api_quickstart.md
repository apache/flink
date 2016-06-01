---
title: "Quickstart: Java API"
# Top navigation
top-nav-group: quickstart
top-nav-pos: 3
top-nav-title: Java API
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

Start working on your Flink Java program in a few simple steps.


## Requirements

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 7.x__ (or higher) installations.

## Create Project

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
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion={{site.version}}
    {% endhighlight %}
        This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
    </div>
    <div class="tab-pane" id="quickstart-script">
    {% highlight bash %}
    $ curl https://flink.apache.org/q/quickstart.sh | bash
    {% endhighlight %}
    </div>
</div>

## Inspect Project

There will be a new directory in your working directory. If you've used the _curl_ approach, the directory is called `quickstart`. Otherwise, it has the name of your artifactId.

The sample project is a __Maven project__, which contains four classes. _StreamingJob_ and _BatchJob_ are basic skeleton programs, _SocketTextStreamWordCount_ is a working streaming example and _WordCountJob_ is a working batch example. Please note that the _main_ method of all classes allow you to start Flink in a development/testing mode.

We recommend you __import this project into your IDE__ to develop and test it. If you use Eclipse, the [m2e plugin](http://www.eclipse.org/m2e/) allows to [import Maven projects](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import). Some Eclipse bundles include that plugin by default, others require you to install it manually. The IntelliJ IDE also supports Maven projects out of the box.


A note to Mac OS X users: The default JVM heapsize for Java is too small for Flink. You have to manually increase it. Choose "Run Configurations" -> Arguments and write into the "VM Arguments" box: "-Xmx800m" in Eclipse.

## Build Project

If you want to __build your project__, go to your project directory and issue the `mvn clean install -Pbuild-jar` command. You will __find a jar__ that runs on every Flink cluster in __target/your-artifact-id-1.0-SNAPSHOT.jar__. There is also a fat-jar,  __target/your-artifact-id-1.0-SNAPSHOT-flink-fat-jar.jar__. This
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

~~~java
public class WordCount {
  
  public static void main(String[] args) throws Exception {
    
    // set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    // get input data
    DataSet<String> text = env.fromElements(
        "To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,"
        );
    
    DataSet<Tuple2<String, Integer>> counts = 
        // split up the lines in pairs (2-tuples) containing: (word,1)
        text.flatMap(new LineSplitter())
        // group by the tuple field "0" and sum up tuple field "1"
        .groupBy(0)
        .aggregate(Aggregations.SUM, 1);

    // emit result
    counts.print();
  }
}
~~~

The operations are defined by specialized classes, here the LineSplitter class.

~~~java
public class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
    // normalize and split the line into words
    String[] tokens = value.toLowerCase().split("\\W+");
    
    // emit the pairs
    for (String token : tokens) {
      if (token.length() > 0) {
        out.collect(new Tuple2<String, Integer>(token, 1));
      }
    }
  }
}
~~~

{% gh_link /flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/wordcount/WordCount.java "Check GitHub" %} for the full example code.

For a complete overview over our API, have a look at the [Programming Guide]({{ site.baseurl }}/apis/programming_guide.html) and [further example programs](examples.html). If you have any trouble, ask on our [Mailing List](http://mail-archives.apache.org/mod_mbox/flink-dev/). We are happy to provide help.

