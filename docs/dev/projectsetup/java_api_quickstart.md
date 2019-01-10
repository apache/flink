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

Start working on your Flink Java program in a few simple steps.


## Requirements

The only requirements are working __Maven 3.0.4__ (or higher) and __Java 8.x__ installations.

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

## Inspect Project

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

*A note to Mac OS X users*: The default JVM heapsize for Java may be too
small for Flink. You have to manually increase it.
In Eclipse, choose
`Run Configurations -> Arguments` and write into the `VM Arguments`
box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu. See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details. 

## Build Project

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
take a look at the [Stream Processing Application Tutorial]({{ site.baseurl }}/tutorials/datastream_api.html#writing-a-flink-program).

If you are writing a batch processing application and you are looking for inspiration what to write,
take a look at the [Batch Application Examples]({{ site.baseurl }}/dev/batch/examples.html).

For a complete overview over the APIs, have a look at the
[DataStream API]({{ site.baseurl }}/dev/datastream_api.html) and
[DataSet API]({{ site.baseurl }}/dev/batch/index.html) sections.

[Here]({{ site.baseurl }}/tutorials/local_setup.html) you can find out how to run an application outside the IDE on a local cluster.

If you have any trouble, ask on our
[Mailing List](http://mail-archives.apache.org/mod_mbox/flink-user/).
We are happy to provide help.

{% top %}
