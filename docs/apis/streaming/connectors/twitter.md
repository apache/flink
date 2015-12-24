---
title: "Twitter Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 5
sub-nav-title: Twitter
---

Twitter Streaming API provides opportunity to connect to the stream of tweets made available by Twitter. Flink Streaming comes with a built-in `TwitterSource` class for establishing a connection to this stream. To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-twitter</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Authentication
In order to connect to Twitter stream the user has to register their program and acquire the necessary information for the authentication. The process is described below.

#### Acquiring the authentication information
First of all, a Twitter account is needed. Sign up for free at [twitter.com/signup](https://twitter.com/signup) or sign in at Twitter's [Application Management](https://apps.twitter.com/) and register the application by clicking on the "Create New App" button. Fill out a form about your program and accept the Terms and Conditions.
After selecting the application, the API key and API secret (called `consumerKey` and `consumerSecret` in `TwitterSource` respectively) is located on the "API Keys" tab. The necessary OAuth Access Token data (`token` and `secret` in `TwitterSource`) can be generated and acquired on the "Keys and Access Tokens" tab.
Remember to keep these pieces of information secret and do not push them to public repositories.

#### Accessing the authentication information
Create a properties file, and pass its path in the constructor of `TwitterSource`. The content of the file should be similar to this:

~~~bash
#properties file for my app
secret=***
consumerSecret=***
token=***-***
consumerKey=***
~~~

#### Constructors
The `TwitterSource` class has two constructors.

1. `public TwitterSource(String authPath, int numberOfTweets);`
to emit a finite number of tweets
2. `public TwitterSource(String authPath);`
for streaming

Both constructors expect a `String authPath` argument determining the location of the properties file containing the authentication information. In the first case, `numberOfTweets` determines how many tweet the source emits.

#### Usage
In contrast to other connectors, the `TwitterSource` depends on no additional services. For example the following code should run gracefully:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"))
{% endhighlight %}
</div>
</div>

The `TwitterSource` emits strings containing a JSON code.
To retrieve information from the JSON code you can add a FlatMap or a Map function handling JSON code. For example, there is an implementation `JSONParseFlatMap` abstract class among the examples. `JSONParseFlatMap` is an extension of the `FlatMapFunction` and has a

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
String getField(String jsonText, String field);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
getField(jsonText : String, field : String) : String
{% endhighlight %}
</div>
</div>

function which can be use to acquire the value of a given field.

There are two basic types of tweets. The usual tweets contain information such as date and time of creation, id, user, language and many more details. The other type is the delete information.

#### Example
`TwitterStream` is an example of how to use `TwitterSource`. It implements a language frequency counter program.
