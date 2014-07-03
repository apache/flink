---
title: "Quickstart: Scala API"
---

Start working on your Stratosphere Scala program in a few simple steps.

#Requirements
The only requirements are working __Maven 3.0.4__ (or higher) and __Java 6.x__ (or higher) installations.


#Create Project
Use one of the following commands to __create a project__:

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#quickstart-script" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
    <li><a href="#maven-archetype" data-toggle="tab">Use <strong>Maven archetypes</strong></a></li>
</ul>
<div class="tab-content">
    <div class="tab-pane active" id="quickstart-script">
{% highlight bash %}
$ curl https://raw.githubusercontent.com/apache/incubator-flink/master/stratosphere-quickstart/quickstart-scala.sh | bash
{% endhighlight %}
    </div>
    <div class="tab-pane" id="maven-archetype">
{% highlight bash %}
$ mvn archetype:generate                             \
  -DarchetypeGroupId=eu.stratosphere               \
  -DarchetypeArtifactId=quickstart-scala           \
  -DarchetypeVersion={{site.FLINK_VERSION_STABLE}}                  
{% endhighlight %}
    This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
    </div>
</div>


#Inspect Project
There will be a __new directory in your working directory__. If you've used the _curl_ approach, the directory is called `quickstart`. Otherwise, it has the name of your artifactId.

The sample project is a __Maven project__, which contains a sample scala _job_ that implements Word Count. Please note that the _RunJobLocal_ and _RunJobRemote_ objects allow you to start Stratosphere in a development/testing mode.</p>

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


# Build Project

If you want to __build your project__, go to your project directory and issue the`mvn clean package` command. You will __find a jar__ that runs on every Stratosphere cluster in __target/stratosphere-project-0.1-SNAPSHOT.jar__.

#Next Steps

__Write your application!__
If you have any trouble, ask on our [Jira page](https://issues.apache.org/jira/browse/FLINK) (open an issue) or on our Mailing list. We are happy to provide help.

