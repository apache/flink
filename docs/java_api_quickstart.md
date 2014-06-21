---
title: "Quickstart: Java API"
---

<p class="lead">Start working on your Stratosphere Java program in a few simple steps.</p>

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
$ curl https://raw.githubusercontent.com/stratosphere/stratosphere-quickstart/master/quickstart.sh | bash
{% endhighlight %}
      </div>
      <div class="tab-pane" id="maven-archetype">
{% highlight bash %}
$ mvn archetype:generate                             \
    -DarchetypeGroupId=eu.stratosphere               \
    -DarchetypeArtifactId=quickstart-java            \
    -DarchetypeVersion={{site.current_stable}}
{% endhighlight %}
      This allows you to <strong>name your newly created project</strong>. It will interactively ask you for the groupId, artifactId, and package name.
      </div>
  </div>
</section>

<section id="inspect_project">
  <div class="page-header"><h2>Inspect Project</h2></div>
  <p class="lead">There will be a <strong>new directory in your working directory</strong>. If you've used the <em>curl</em> approach, the directory is called <code>quickstart</code>. Otherwise, it has the name of your artifactId.</p>
  <p class="lead">The sample project is a <strong>Maven project</strong>, which contains two classes. <em>Job</em> is a basic skeleton program and <em>WordCountJob</em> a working example. Please note that the <em>main</em> method of both classes allow you to start Stratosphere in a development/testing mode.</p>
  <p class="lead">We recommend to <strong>import this project into your IDE</strong> to develop and test it. If you use Eclipse, the <a href="http://www.eclipse.org/m2e/">m2e plugin</a> allows to <a href="http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import">import Maven projects</a>. Some Eclipse bundles include that plugin by default, other require you to install it manually. The IntelliJ IDE also supports Maven projects out of the box.</p>
</section>

<section id="build_project">
<div class="alert alert-danger">A note to Mac OS X users: The default JVM heapsize for Java is too small for Stratosphere. You have to manually increase it. Choose "Run Configurations" -> Arguments and write into the "VM Arguments" box: "-Xmx800m" in Eclipse.</div>
  <div class="page-header"><h2>Build Project</h2></div>
  <p class="lead">If you want to <strong>build your project</strong>, go to your project directory and issue the <code>mvn clean package</code> command. You will <strong>find a jar</strong> that runs on every Stratosphere cluster in <code>target/stratosphere-project-0.1-SNAPSHOT.jar</code>.</p>
</section>

<section id="next_steps">
  <div class="page-header"><h2>Next Steps</h2></div>
  <p class="lead"><strong>Write your application!</strong></p>
  <p>The quickstart project contains a WordCount implementation, the "Hello World" of Big Data processing systems. The goal of WordCount is to determine the frequencies of words in a text, e.g., how often do the terms "the" or "house" occurs in all Wikipedia texts.</p>
 <br>
<b>Sample Input:</b> <br>
{% highlight bash %}
big data is big
{% endhighlight %}
<b>Sample Output:</b> <br>
{% highlight bash %}
big 2
data 1
is 1
{% endhighlight %}

<p>The following code shows the WordCount implementation from the Quickstart which processes some text lines with two operators (FlatMap and Reduce), and writes the prints the resulting words and counts to std-out.</p>

{% highlight java %}
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
    
    // execute program
    env.execute("WordCount Example");
  }
}
{% endhighlight %}

<p>The operations are defined by specialized classes, here the LineSplitter class.</p>

{% highlight java %}
public class LineSplitter extends FlatMapFunction<String, Tuple2<String, Integer>> {

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

{% endhighlight %}

<p><a href="https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/wordcount/WordCount.java">Check GitHub</a> for the full example code.</p>

<p class="lead">For a complete overview over our Java API, have a look at the <a href="{{ site.baseurl }}/docs/{{site.current_stable_documentation}}/programming_guides/java.html">Stratosphere Documentation</a> and <a href="{{ site.baseurl }}/docs/{{site.current_stable_documentation}}/programming_guides/examples_java.html">further example programs</a>. If you have any trouble, ask on our <a href="https://groups.google.com/forum/#!forum/stratosphere-dev">Mailing list</a>. We are happy to provide help.</p>
</section>
