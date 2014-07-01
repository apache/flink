---
title:  "Local Execution"
---

# Local Execution/Debugging

Stratosphere can run on a single machine, even in a single Java Virtual Machine. This allows users to test and debug Stratosphere programs locally. This section gives an overview of the local execution mechanisms.

**NOTE:** Please also refer to the [debugging section](java_api_guide.html#debugging) in the Java API documentation for a guide to testing and local debugging utilities in the Java API.

The local environments and executors allow you to run Stratosphere programs in local Java Virtual Machine, or with within any JVM as part of existing programs. Most examples can be launched locally by simply hitting the "Run" button of your IDE.

If you are running Stratosphere programs locally, you can also debug your program like any other Java program. You can either use `System.out.println()` to write out some internal variables or you can use the debugger. It is possible to set breakpoints within `map()`, `reduce()` and all the other methods.

The `JobExecutionResult` object, which is returned after the execution finished, contains the program runtime and the accumulator results.

*Note:* The local execution environments do not start any web frontend to monitor the execution.


# Maven Dependency

If you are developing your program in a Maven project, you have to add the `stratosphere-clients` module using this dependency:

```xml
<dependency>
  <groupId>eu.stratosphere</groupId>
  <artifactId>stratosphere-clients</artifactId>
  <version>{{site.current_stable}}</version>
</dependency>
```

# Local Environment

The `LocalEnvironment` is a handle to local execution for Stratosphere programs. Use it to run a program within a local JVM - standalone or embedded in other programs.

The local environment is instantiated via the method `ExecutionEnvironment.createLocalEnvironment()`. By default, it will use as many local threads for execution as your machine has CPU cores (hardware contexts). You can alternatively specify the desired parallelism. The local environment can be configured to log to the console using `enableLogging()`/`disableLogging()`.

In most cases, calling `ExecutionEnvironment.getExecutionEnvironment()` is the even better way to go. That method returns a `LocalEnvironment` when the program is started locally (outside the command line interface), and it returns a pre-configured environment for cluster execution, when the program is invoked by the [command line interface](cli.html).

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

    DataSet<String> data = env.readTextFile("file:///path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("file:///path/to/result");

    env.execute();
}
```


# Local Executor

The *LocalExecutor* is similar to the local environment, but it takes a *Plan* object, which describes the program as a single executable unit. The *LocalExecutor* is typically used with the Scala API. 

The following code shows how you would use the `LocalExecutor` with the Wordcount example for Scala Programs:

```scala
public static void main(String[] args) throws Exception {
    val input = TextFile("hdfs://path/to/file")

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } }
    val counts = words groupBy { x => x } count()

    val output = counts.write(wordsOutput, CsvOutputFormat())
  
    val plan = new ScalaPlan(Seq(output), "Word Count")
    LocalExecutor.executePlan(p);
}
```


# LocalDistributedExecutor

Stratosphere also offers a `LocalDistributedExecutor` which starts multiple TaskManagers within one JVM. The standard `LocalExecutor` starts one JobManager and one TaskManager in one JVM.
With the `LocalDistributedExecutor` you can define the number of TaskManagers to start. This is useful for debugging network related code and more of a developer tool than a user tool.

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<String> data = env.readTextFile("hdfs://path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("hdfs://path/to/result");

    Plan p = env.createProgramPlan();
    LocalDistributedExecutor lde = new LocalDistributedExecutor();
    lde.startNephele(2); // start two TaskManagers
    lde.run(p);
}
```


