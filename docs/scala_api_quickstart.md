---
title: "Quick Start: Scala API"
---

<p class="lead">Start working on your Stratosphere Scala program in a few simple steps.</p>

<section id="requirements">
  <div class="page-header"><h2>Requirements</h2></div>
  <p class="lead">The only requirements are working <strong>Maven 3.0.4</strong> (or higher) and <strong>Java 6.x</strong> (or higher) installations.</p>
</section>

<section id="create_project">
  <div class="page-header"><h2>Create Project</h2></div>
  <p class="lead">Use one of the following commands to <strong>create a project</strong>:</p>

  <ul class="nav nav-tabs" style="border-bottom: none;">
      <li class="active"><a href="#quickstart-script" data-toggle="tab">Run the <strong>quickstart script</strong></a></li>
      <li><a href="#maven-archetype" data-toggle="tab">Use <strong>Maven archetypes</strong></a></li>
  </ul>
  <div class="tab-content">
      <div class="tab-pane active" id="quickstart-script">
{% highlight bash %}
$ curl https://raw.githubusercontent.com/stratosphere/stratosphere-quickstart/master/quickstart-scala.sh | bash
{% endhighlight %}
      </div>
      <div class="tab-pane" id="maven-archetype">
{% highlight bash %}
$ mvn archetype:generate                             \
    -DarchetypeGroupId=eu.stratosphere               \
    -DarchetypeArtifactId=quickstart-scala           \
    -DarchetypeVersion={{site.current_stable}}                  
{% endhighlight %}
      This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
      </div>
  </div>
</section>

<section id="inspect_project">
  <div class="page-header"><h2>Inspect Project</h2></div>
  <p class="lead">There will be a <strong>new directory in your working directory</strong>. If you've used the <em>curl</em> approach, the directory is called <code>quickstart</code>. Otherwise, it has the name of your artifactId.</p>
  <p class="lead">The sample project is a <strong>Maven project</strong>, which contains a sample scala <em>Job</em> that implements Word Count. Please note that the <em>RunJobLocal</em> and <em>RunJobRemote</em> objects allow you to start Stratosphere in a development/testing mode.</p>
  <p class="lead">We recommend to <strong>import this project into your IDE</strong>. For Eclipse, you need the following plugins, which you can install from the provided Eclipse Update Sites:
    <ul>
      <li class="lead"><strong>Eclipse 4.x</strong>:
        <ul>
          <li><strong>Scala IDE</strong> <small>(http://download.scala-ide.org/sdk/e38/scala210/stable/site)</small></li>
          <li><strong>m2eclipse-scala</strong> <small>(http://alchim31.free.fr/m2e-scala/update-site)</small></li>
          <li><strong>Build Helper Maven Plugin</strong> <small>(https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.15.0/N/0.15.0.201206251206/)</small></li>
        </ul>
      </li>
      <li class="lead"><strong>Eclipse 3.7</strong>:
        <ul>
          <li><strong>Scala IDE</strong> <small>(http://download.scala-ide.org/sdk/e37/scala210/stable/site)</small></li>
          <li><strong>m2eclipse-scala</strong> <small>(http://alchim31.free.fr/m2e-scala/update-site)</small></li>
          <li><strong>Build Helper Maven Plugin</strong> <small>(https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/)</small></li>
        </ul>
      </li>
    </ul>
  </p>
  <p class="lead">The IntelliJ IDE also supports Maven and offers a plugin for Scala development.</p>
</section>

<section id="build_project">
  <div class="page-header"><h2>Build Project</h2></div>
  <p class="lead">If you want to <strong>build your project</strong>, go to your project directory and issue the <code>mvn clean package</code> command. You will <strong>find a jar</strong> that runs on every Stratosphere cluster in <code>target/stratosphere-project-0.1-SNAPSHOT.jar</code>.</p>
</section>

<section id="next_steps">
  <div class="page-header"><h2>Next Steps</h2></div>
  <p class="lead"><strong>Write your application!</strong> If you have any trouble, ask on our <a href="https://github.com/stratosphere/stratosphere/issues">GitHub page</a> (open an issue) or on our <a href="https://groups.google.com/forum/#!forum/stratosphere-dev">Mailing list</a>. We are happy to provide help.</p>
</p>
