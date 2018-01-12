---
title: "Java 8"
nav-parent_id: api-concepts
nav-pos: 20
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

Java 8 introduces several new language features designed for faster and clearer coding. With the most important feature,
the so-called "Lambda Expressions", Java 8 opens the door to functional programming. Lambda Expressions allow for implementing and
passing functions in a straightforward way without having to declare additional (anonymous) classes.

The newest version of Flink supports the usage of Lambda Expressions for all operators of the Java API.
This document shows how to use Lambda Expressions and describes current limitations. For a general introduction to the
Flink API, please refer to the [Programming Guide]({{ site.baseurl }}/dev/api_concepts.html)

* TOC
{:toc}

### Examples

The following example illustrates how to implement a simple, inline `map()` function that squares its input using a Lambda Expression.
The types of input `i` and output parameters of the `map()` function need not to be declared as they are inferred by the Java 8 compiler.

~~~java
env.fromElements(1, 2, 3)
// returns the squared i
.map(i -> i*i)
.print();
~~~

The next two examples show different implementations of a function that uses a `Collector` for output.
Functions, such as `flatMap()`, require an output type (in this case `String`) to be defined for the `Collector` in order to be type-safe.
If the `Collector` type can not be inferred from the surrounding context, it needs to be declared in the Lambda Expression's parameter list manually.
Otherwise the output will be treated as type `Object` which can lead to undesired behaviour.

~~~java
DataSet<Integer> input = env.fromElements(1, 2, 3);

// collector type must be declared
input.flatMap((Integer number, Collector<String> out) -> {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < number; i++) {
        builder.append("a");
        out.collect(builder.toString());
    }
})
// returns (on separate lines) "a", "a", "aa", "a", "aa", "aaa"
.print();
~~~

~~~java
DataSet<Integer> input = env.fromElements(1, 2, 3);

// collector type must not be declared, it is inferred from the type of the dataset
DataSet<String> manyALetters = input.flatMap((number, out) -> {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < number; i++) {
       builder.append("a");
       out.collect(builder.toString());
    }
});

// returns (on separate lines) "a", "a", "aa", "a", "aa", "aaa"
manyALetters.print();
~~~

The following code demonstrates a word count which makes extensive use of Lambda Expressions.

~~~java
DataSet<String> input = env.fromElements("Please count", "the words", "but not this");

// filter out strings that contain "not"
input.filter(line -> !line.contains("not"))
// split each line by space
.map(line -> line.split(" "))
// emit a pair <word,1> for each array element
.flatMap((String[] wordArray, Collector<Tuple2<String, Integer>> out)
    -> Arrays.stream(wordArray).forEach(t -> out.collect(new Tuple2<>(t, 1)))
    )
// group and sum up
.groupBy(0).sum(1)
// print
.print();
~~~

### Compiler Limitations
Currently, Flink only supports jobs containing Lambda Expressions completely if they are **compiled with the Eclipse JDT compiler contained in Eclipse Luna 4.4.2 (and above)**.

Only the Eclipse JDT compiler preserves the generic type information necessary to use the entire Lambda Expressions feature type-safely.
Other compilers such as the OpenJDK's and Oracle JDK's `javac` throw away all generic parameters related to Lambda Expressions. This means that types such as `Tuple2<String, Integer>` or `Collector<String>` declared as a Lambda function input or output parameter will be pruned to `Tuple2` or `Collector` in the compiled `.class` files, which is too little information for the Flink compiler.

How to compile a Flink job that contains Lambda Expressions with the JDT compiler will be covered in the next section.

However, it is possible to implement functions such as `map()` or `filter()` with Lambda Expressions in Java 8 compilers other than the Eclipse JDT compiler as long as the function has no `Collector`s or `Iterable`s *and* only if the function handles unparameterized types such as `Integer`, `Long`, `String`, `MyOwnClass` (types without Generics!).

#### Compile Flink jobs with the Eclipse JDT compiler and Maven

If you are using the Eclipse IDE, you can run and debug your Flink code within the IDE without any problems after some configuration steps. The Eclipse IDE by default compiles its Java sources with the Eclipse JDT compiler. The next section describes how to configure the Eclipse IDE.

If you are using a different IDE such as IntelliJ IDEA or you want to package your Jar-File with Maven to run your job on a cluster, you need to modify your project's `pom.xml` file and build your program with Maven. The [quickstart]({{site.baseurl}}/quickstart/setup_quickstart.html) contains preconfigured Maven projects which can be used for new projects or as a reference. Uncomment the mentioned lines in your generated quickstart `pom.xml` file if you want to use Java 8 with Lambda Expressions.

Alternatively, you can manually insert the following lines to your Maven `pom.xml` file. Maven will then use the Eclipse JDT compiler for compilation.

~~~xml
<!-- put these lines under "project/build/pluginManagement/plugins" of your pom.xml -->

<plugin>
    <!-- Use compiler plugin with tycho as the adapter to the JDT compiler. -->
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <source>1.8</source>
        <target>1.8</target>
        <compilerId>jdt</compilerId>
    </configuration>
    <dependencies>
        <!-- This dependency provides the implementation of compiler "jdt": -->
        <dependency>
            <groupId>org.eclipse.tycho</groupId>
            <artifactId>tycho-compiler-jdt</artifactId>
            <version>0.21.0</version>
        </dependency>
    </dependencies>
</plugin>
~~~

If you are using Eclipse for development, the m2e plugin might complain about the inserted lines above and marks your `pom.xml` as invalid. If so, insert the following lines to your `pom.xml`.

~~~xml
<!-- put these lines under "project/build/pluginManagement/plugins/plugin[groupId="org.eclipse.m2e", artifactId="lifecycle-mapping"]/configuration/lifecycleMappingMetadata/pluginExecutions" of your pom.xml -->

<pluginExecution>
    <pluginExecutionFilter>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <versionRange>[3.1,)</versionRange>
        <goals>
            <goal>testCompile</goal>
            <goal>compile</goal>
        </goals>
    </pluginExecutionFilter>
    <action>
        <ignore></ignore>
    </action>
</pluginExecution>
~~~

#### Run and debug Flink jobs within the Eclipse IDE

First of all, make sure you are running a current version of Eclipse IDE (4.4.2 or later). Also make sure that you have a Java 8 Runtime Environment (JRE) installed in Eclipse IDE (`Window` -> `Preferences` -> `Java` -> `Installed JREs`).

Create/Import your Eclipse project.

If you are using Maven, you also need to change the Java version in your `pom.xml` for the `maven-compiler-plugin`. Otherwise right click the `JRE System Library` section of your project and open the `Properties` window in order to switch to a Java 8 JRE (or above) that supports Lambda Expressions.

The Eclipse JDT compiler needs a special compiler flag in order to store type information in `.class` files. Open the JDT configuration file at `{project directory}/.settings/org.eclipse.jdt.core.prefs` with your favorite text editor and add the following line:

~~~
org.eclipse.jdt.core.compiler.codegen.lambda.genericSignature=generate
~~~

If not already done, also modify the Java versions of the following properties to `1.8` (or above):

~~~
org.eclipse.jdt.core.compiler.codegen.targetPlatform=1.8
org.eclipse.jdt.core.compiler.compliance=1.8
org.eclipse.jdt.core.compiler.source=1.8
~~~

After you have saved the file, perform a complete project refresh in Eclipse IDE.

If you are using Maven, right click your Eclipse project and select `Maven` -> `Update Project...`.

You have configured everything correctly, if the following Flink program runs without exceptions:

~~~java
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.fromElements(1, 2, 3).map((in) -> new Tuple1<String>(" " + in)).print();
env.execute();
~~~

{% top %}
