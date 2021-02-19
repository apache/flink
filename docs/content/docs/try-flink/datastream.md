---
title: "Fraud Detection with the DataStream API"
type: docs
weight: 2
aliaes:
  - /try-flink/datastream_api.html
  - /getting-started/walkthroughs/datastream_api.html
  - /quickstart/run_example_quickstart.htmls
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

# Fraud Detection with the DataStream API

Apache Flink offers a DataStream API for building robust, stateful streaming applications.
It provides fine-grained control over state and time, which allows for the implementation of advanced event-driven systems.
In this step-by-step guide you'll learn how to build a stateful streaming application with Flink's DataStream API.

## What Are You Building? 

Credit card fraud is a growing concern in the digital age.
Criminals steal credit card numbers by running scams or hacking into insecure systems.
Stolen numbers are tested by making one or more small purchases, often for a dollar or less.
If that works, they then make more significant purchases to get items they can sell or keep for themselves.

In this tutorial, you will build a fraud detection system for alerting on suspicious credit card transactions.
Using a simple set of rules, you will see how Flink allows us to implement advanced business logic and act in real-time.

## Prerequisites

This walkthrough assumes that you have some familiarity with Java or Scala, but you should be able to follow along even if you are coming from a different programming language.

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/gettinghelp.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) is consistently ranked as one of the most active of any Apache project and a great way to get help quickly.

## How to Follow Along

If you want to follow along, you will require a computer with:

* Java 8 or 11
* Maven 

A provided Flink Maven Archetype will create a skeleton project with all the necessary dependencies quickly, so you only need to focus on filling out the business logic.
These dependencies include `flink-streaming-java` which is the core dependency for all Flink streaming applications and `flink-walkthrough-common` that has data generators and other classes specific to this walkthrough.

{{< tabs "archetype" >}}
{{< tab "Java" >}}
```bash
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion={{< version >}} \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```
{{< /tab >}}
{{< tab "Scala" >}}
```bash
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-scala \
    -DarchetypeVersion={{< version >}} \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```
{{< /tab >}}
{{< /tabs >}}

{{< unstable >}}
{{< hint warning >}}
For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the command line. For details about this change, please refer to [Maven official documentation](http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html).
If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For example:
```xml
<settings>
  <activeProfiles>
    <activeProfile>apache</activeProfile>
  </activeProfiles>
  <profiles>
    <profile>
      <id>apache</id>
      <repositories>
        <repository>
          <id>apache-snapshots</id>
          <url>https://repository.apache.org/content/repositories/snapshots/</url>
        </repository>
      </repositories>
    </profile>
  </profiles>
</settings>
```
{{< /hint >}}
{{< /unstable >}}


You can edit the `groupId`, `artifactId` and `package` if you like. With the above parameters,
Maven will create a folder named `frauddetection` that contains a project with all the dependencies to complete this tutorial.
After importing the project into your editor, you can find a file `FraudDetectionJob.java` (or `FraudDetectionJob.scala`) with the following code which you can run directly inside your IDE.
Try setting break points through out the data stream and run the code in DEBUG mode to get a feeling for how everything works.

{{< tabs "start" >}}
{{< tab "Java" >}}
#### FraudDetectionJob.java
```java
package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            .name("transactions");
        
        DataStream<Alert> alerts = transactions
            .keyBy(Transaction::getAccountId)
            .process(new FraudDetector())
            .name("fraud-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
```

#### FraudDetector.java
```java
package spendreport;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
#### FraudDetectionJob.scala
```scala
package spendreport

import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transactions")

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts
      .addSink(new AlertSink)
      .name("send-alerts")

    env.execute("Fraud Detection")
  }
}
```

#### FraudDetector.scala
```scala
package spendreport

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @throws[Exception]
  def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {

    val alert = new Alert
    alert.setId(transaction.getAccountId)

    collector.collect(alert)
  }
}
```
{{< /tab >}}
{{< /tabs >}}

## Breaking Down the Code

Let's walk step-by-step through the code of these two files. The `FraudDetectionJob` class defines the data flow of the application and the `FraudDetector` class defines the business logic of the function that detects fraudulent transactions.

We start describing how the Job is assembled in the `main` method of the `FraudDetectionJob` class.

#### The Execution Environment

The first line sets up your `StreamExecutionEnvironment`.
The execution environment is how you set properties for your Job, create your sources, and finally trigger the execution of the Job.

{{< tabs "execution environment" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
```
{{< /tab >}}
{{< /tabs >}}


#### Creating a Source

Sources ingest data from external systems, such as Apache Kafka, Rabbit MQ, or Apache Pulsar, into Flink Jobs.
This walkthrough uses a source that generates an infinite stream of credit card transactions for you to process.
Each transaction contains an account ID (`accountId`), timestamp (`timestamp`) of when the transaction occurred, and US$ amount (`amount`).
The `name` attached to the source is just for debugging purposes, so if something goes wrong, we will know where the error originated.

{{< tabs "source" >}}
{{< tab "Java" >}}
```java
DataStream<Transaction> transactions = env
    .addSource(new TransactionSource())
    .name("transactions");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val transactions: DataStream[Transaction] = env
  .addSource(new TransactionSource)
  .name("transactions")
```
{{< /tab >}}
{{< /tabs >}}

#### Partitioning Events & Detecting Fraud

The `transactions` stream contains a lot of transactions from a large number of users, such that it needs to be processed in parallel by multiple fraud detection tasks. Since fraud occurs on a per-account basis, you must ensure that all transactions for the same account are processed by the same parallel task of the fraud detector operator.

To ensure that the same physical task processes all records for a particular key, you can partition a stream using `DataStream#keyBy`. 
The `process()` call adds an operator that applies a function to each partitioned element in the stream.
It is common to say the operator immediately after a `keyBy`, in this case `FraudDetector`, is executed within a _keyed context_.

{{< tabs "transformation" >}}
{{< tab "Java" >}}
```java
DataStream<Alert> alerts = transactions
    .keyBy(Transaction::getAccountId)
    .process(new FraudDetector())
    .name("fraud-detector");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val alerts: DataStream[Alert] = transactions
  .keyBy(transaction => transaction.getAccountId)
  .process(new FraudDetector)
  .name("fraud-detector")
```
{{< /tab >}}
{{< /tabs >}}

#### Outputting Results
 
A sink writes a `DataStream` to an external system; such as Apache Kafka, Cassandra, and AWS Kinesis.
The `AlertSink` logs each `Alert` record with log level **INFO**, instead of writing it to persistent storage, so you can easily see your results.

{{< tabs "sink" >}}
{{< tab "Java" >}}
```java
alerts.addSink(new AlertSink());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
alerts.addSink(new AlertSink)
```
{{< /tab >}}
{{< /tabs >}}

#### The Fraud Detector

The fraud detector is implemented as a `KeyedProcessFunction`.
Its method `KeyedProcessFunction#processElement` is called for every transaction event.
This first version produces an alert on every transaction, which some may say is overly conservative.

The next steps of this tutorial will guide you to expand the fraud detector with more meaningful business logic.

{{< tabs "detector" >}}
{{< tab "Java" >}}
```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {
  
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @throws[Exception]
  def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {

    val alert = new Alert
    alert.setId(transaction.getAccountId)

    collector.collect(alert)
  }
}
```
{{< /tab >}}
{{< /tabs >}}

## Writing a Real Application (v1)

For the first version, the fraud detector should output an alert for any account that makes a small transaction immediately followed by a large one. Where small is anything less than $1.00 and large is more than $500.
Imagine your fraud detector processes the following stream of transactions for a particular account.


### Expected Output

Running this code with the provided `TransactionSource` will emit fraud alerts for account 3.
You should see the following output in your task manager logs: 

```bash
2019-08-19 14:22:06,220 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:11,383 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:16,551 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:21,723 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:26,896 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
```
