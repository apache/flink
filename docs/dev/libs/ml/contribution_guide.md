---
mathjax: include
title: How to Contribute
nav-parent_id: ml
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

The Flink community highly appreciates all sorts of contributions to FlinkML.
FlinkML offers people interested in machine learning to work on a highly active open source project which makes scalable ML reality.
The following document describes how to contribute to FlinkML.

* This will be replaced by the TOC
{:toc}

## Getting Started

In order to get started first read Flink's [contribution guide](http://flink.apache.org/how-to-contribute.html).
Everything from this guide also applies to FlinkML.

## Pick a Topic

If you are looking for some new ideas you should first look into our [roadmap](https://cwiki.apache.org/confluence/display/FLINK/FlinkML%3A+Vision+and+Roadmap), then you should check out the list of [unresolved issues on JIRA](https://issues.apache.org/jira/issues/?jql=component%20%3D%20%22Machine%20Learning%20Library%22%20AND%20project%20%3D%20FLINK%20AND%20resolution%20%3D%20Unresolved%20ORDER%20BY%20priority%20DESC).
Once you decide to contribute to one of these issues, you should take ownership of it and track your progress with this issue.
That way, the other contributors know the state of the different issues and redundant work is avoided.

If you already know what you want to contribute to FlinkML all the better.
It is still advisable to create a JIRA issue for your idea to tell the Flink community what you want to do, though.

## Testing

New contributions should come with tests to verify the correct behavior of the algorithm.
The tests help to maintain the algorithm's correctness throughout code changes, e.g. refactorings.

We distinguish between unit tests, which are executed during Maven's test phase, and integration tests, which are executed during maven's verify phase.
Maven automatically makes this distinction by using the following naming rules:
All test cases whose class name ends with a suffix fulfilling the regular expression `(IT|Integration)(Test|Suite|Case)`, are considered integration tests.
The rest are considered unit tests and should only test behavior which is local to the component under test.

An integration test is a test which requires the full Flink system to be started.
In order to do that properly, all integration test cases have to mix in the trait `FlinkTestBase`.
This trait will set the right `ExecutionEnvironment` so that the test will be executed on a special `FlinkMiniCluster` designated for testing purposes.
Thus, an integration test could look the following:

{% highlight scala %}
class ExampleITSuite extends FlatSpec with FlinkTestBase {
  behavior of "An example algorithm"

  it should "do something" in {
    ...
  }
}
{% endhighlight %}

The test style does not have to be `FlatSpec` but can be any other scalatest `Suite` subclass.
See [ScalaTest testing styles](http://scalatest.org/user_guide/selecting_a_style) for more information.

## Documentation

When contributing new algorithms, it is required to add code comments describing the way the algorithm works and its parameters with which the user can control its behavior.
Additionally, we would like to encourage contributors to add this information to the online documentation.
The online documentation for FlinkML's components can be found in the directory `docs/libs/ml`.

Every new algorithm is described by a single markdown file.
This file should contain at least the following points:

1. What does the algorithm do
2. How does the algorithm work (or reference to description)
3. Parameter description with default values
4. Code snippet showing how the algorithm is used

In order to use latex syntax in the markdown file, you have to include `mathjax: include` in the YAML front matter.

{% highlight java %}
---
mathjax: include
htmlTitle: FlinkML - Example title
title: <a href="../ml">FlinkML</a> - Example title
---
{% endhighlight %}

In order to use displayed mathematics, you have to put your latex code in `$$ ... $$`.
For in-line mathematics, use `$ ... $`.
Additionally some predefined latex commands are included into the scope of your markdown file.
See `docs/_include/latex_commands.html` for the complete list of predefined latex commands.

## Contributing

Once you have implemented the algorithm with adequate test coverage and added documentation, you are ready to open a pull request.
Details of how to open a pull request can be found [here](http://flink.apache.org/how-to-contribute.html#contributing-code--documentation).

{% top %}
