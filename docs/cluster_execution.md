---
title:  "Cluster Execution"
---

Stratosphere programs can run distributed on clusters of many machines. There
are two ways to send a program to a cluster for execution:

# Command Line Interface

The command line interface lets you submit packaged programs (JARs) to a cluster
(or single machine setup).

Please refer to the [Command Line Interface](cli.html) documentation for
details.

# Remote Environment

The remote environment lets you execute Stratosphere Java programs on a cluster
directly. The remote environment points to the cluster on which you want to
execute the program.

## Maven Dependency

If you are developing your program as a Maven project, you have to add the
`stratosphere-clients` module using this dependency:

```xml
<dependency>
  <groupId>eu.stratosphere</groupId>
  <artifactId>stratosphere-clients</artifactId>
  <version>{{ site.FLINK_VERSION }}</version>
</dependency>
```

## Example

The following illustrates the use of the `RemoteEnvironment`:

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment
        .createRemoteEnvironment("strato-master", "7661", "/home/user/udfs.jar");

    DataSet<String> data = env.readTextFile("hdfs://path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("hdfs://path/to/result");

    env.execute();
}
```

Note that the program contains custom UDFs and hence requires a JAR file with
the classes of the code attached. The constructor of the remote environment
takes the path(s) to the JAR file(s).

# Remote Executor

Similar to the RemoteEnvironment, the RemoteExecutor lets you execute
Stratosphere programs on a cluster directly. The remote executor accepts a
*Plan* object, which describes the program as a single executable unit.

## Maven Dependency

If you are developing your program in a Maven project, you have to add the
`stratosphere-clients` module using this dependency:

```xml
<dependency>
  <groupId>eu.stratosphere</groupId>
  <artifactId>stratosphere-clients</artifactId>
  <version>{{ site.FLINK_VERSION }}</version>
</dependency>
```

## Example

The following illustrates the use of the `RemoteExecutor` with the Scala API:

```scala
def main(args: Array[String]) {
    val input = TextFile("hdfs://path/to/file")

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } }
    val counts = words groupBy { x => x } count()

    val output = counts.write(wordsOutput, CsvOutputFormat())
  
    val plan = new ScalaPlan(Seq(output), "Word Count")
    val executor = new RemoteExecutor("strato-master", 7881, "/path/to/jarfile.jar")
    executor.executePlan(p);
}
```

The following illustrates the use of the `RemoteExecutor` with the Java API (as
an alternative to the RemoteEnvironment):

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
    RemoteExecutor e = new RemoteExecutor("strato-master", 7881, "/path/to/jarfile.jar");
    e.executePlan(p);
}
```

Note that the program contains custom UDFs and hence requires a JAR file with
the classes of the code attached. The constructor of the remote executor takes
the path(s) to the JAR file(s).
