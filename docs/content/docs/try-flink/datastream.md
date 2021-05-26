---
title: "Fraud Detection with the DataStream API"
type: docs
weight: 2
aliases:
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

{{< img src="/fig/fraud-transactions.svg" alt="Fraud Transaction" >}}

Transactions 3 and 4 should be marked as fraudulent because it is a small transaction, $0.09, followed by a large one, $510. Alternatively, transactions 7, 8, and 9 are not fraud because the small amount of $0.02 is not immediately followed by the large one; instead, there is an intermediate transaction that breaks the pattern.

To do this, the fraud detector must _remember_ information across events; a large transaction is only fraudulent if the previous one was small.
Remembering information across events requires [state]({{< ref "docs/concepts/glossary#managed-state" >}}), and that is why we decided to use a [KeyedProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}}). 
It provides fine-grained control over both state and time, which will allow us to evolve our algorithm with more complex requirements throughout this walkthrough.

The most straightforward implementation is a boolean flag that is set whenever a small transaction is processed.
When a large transaction comes through, you can simply check if the flag is set for that account.

However, merely implementing the flag as a member variable in the `FraudDetector` class will not work. 
Flink processes the transactions of multiple accounts with the same object instance of `FraudDetector`, which means if accounts A and B are routed through the same instance of `FraudDetector`, a transaction for account A could set the flag to true, and then a transaction for account B could set off a false alert. 
We could of course use a data structure like a `Map` to keep track of the flags for individual keys, however, a simple member variable would not be fault-tolerant and all its information be lost in case of a failure.
Hence, the fraud detector would possibly miss alerts if the application ever had to restart to recover from a failure.

To address these challenges, Flink provides primitives for a fault-tolerant state that are almost as easy to use as regular member variables.

The most basic type of state in Flink is [ValueState]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}), a data type that adds fault tolerance to any variable it wraps.
`ValueState` is a form of _keyed state_, meaning it is only available in operators that are applied in a _keyed context_; any operator immediately following `DataStream#keyBy`.
A _keyed state_ of an operator is automatically scoped to the key of the record that is currently processed.
In this example, the key is the account id for the current transaction (as declared by `keyBy()`), and `FraudDetector` maintains an independent state for each account. 
`ValueState` is created using a `ValueStateDescriptor` which contains metadata about how Flink should manage the variable. The state should be registered before the function starts processing data.
The right hook for this is the `open()` method.

{{< tabs "detector-v1" >}}
{{< tab "Java" >}}
```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
    }
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)
  }
```
{{< /tab >}}
{{< /tabs >}}

`ValueState` is a wrapper class, similar to `AtomicReference` or `AtomicLong` in the Java standard library.
It provides three methods for interacting with its contents; `update` sets the state, `value` gets the current value, and `clear` deletes its contents.
If the state for a particular key is empty, such as at the beginning of an application or after calling `ValueState#clear`, then `ValueState#value` will return `null`.
Modifications to the object returned by `ValueState#value` are not guaranteed to be recognized by the system, and so all changes must be performed with `ValueState#update`.
Otherwise, fault tolerance is managed automatically by Flink under the hood, and so you can interact with it like with any standard variable.

Below, you can see an example of how you can use a flag state to track potential fraudulent transactions.

{{< tabs "processelement-v1" >}}
{{< tab "Java" >}}
```java
@Override
public void processElement(
        Transaction transaction,
        Context context,
        Collector<Alert> collector) throws Exception {

    // Get the current state for the current key
    Boolean lastTransactionWasSmall = flagState.value();

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
        if (transaction.getAmount() > LARGE_AMOUNT) {
            // Output an alert downstream
            Alert alert = new Alert();
            alert.setId(transaction.getAccountId());

            collector.collect(alert);            
        }

        // Clean up our state
        flagState.clear();
    }

    if (transaction.getAmount() < SMALL_AMOUNT) {
        // Set the flag to true
        flagState.update(true);
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
override def processElement(
    transaction: Transaction,
    context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
    collector: Collector[Alert]): Unit = {

  // Get the current state for the current key
  val lastTransactionWasSmall = flagState.value

  // Check if the flag is set
  if (lastTransactionWasSmall != null) {
    if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
      // Output an alert downstream
      val alert = new Alert
      alert.setId(transaction.getAccountId)

      collector.collect(alert)
    }
    // Clean up our state
    flagState.clear()
  }

  if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
    // set the flag to true
    flagState.update(true)
  }
}
```
{{< /tab >}}
{{< /tabs >}}

For every transaction, the fraud detector checks the state of the flag for that account.
Remember, `ValueState` is always scoped to the current key, i.e., account.
If the flag is non-null, then the last transaction seen for that account was small, and so if the amount for this transaction is large, then the detector outputs a fraud alert.

After that check, the flag state is unconditionally cleared.
Either the current transaction caused a fraud alert, and the pattern is over, or the current transaction did not cause an alert, and the pattern is broken and needs to be restarted.

Finally, the transaction amount is checked to see if it is small.
If so, then the flag is set so that it can be checked by the next event.
Notice that `ValueState<Boolean>` has three states, unset (`null`), `true`, and `false`, because all `ValueState`'s are nullable.
This job only makes use of unset (`null`) and `true` to check whether the flag is set or not.

## Fraud Detector v2: State + Time = &#10084;&#65039;

Scammers don't wait long to make their large purchases to reduce the chances their test transaction is noticed. 
For example, suppose you wanted to set a 1-minute timeout to your fraud detector; i.e., in the previous example transactions 3 and 4 would only be considered fraud if they occurred within 1 minute of each other.
Flink's `KeyedProcessFunction` allows you to set timers that invoke a callback method at some point in time in the future.

Let's see how we can modify our Job to comply with our new requirements:

* Whenever the flag is set to `true`, also set a timer for 1 minute in the future.
* When the timer fires, reset the flag by clearing its state.
* If the flag is ever cleared the timer should be canceled.

To cancel a timer, you have to remember what time it is set for, and remembering implies state, so you will begin by creating a timer state along with your flag state.

{{< tabs "state-v2" >}}
{{< tab "Java" >}}
```java
private transient ValueState<Boolean> flagState;
private transient ValueState<Long> timerState;

@Override
public void open(Configuration parameters) {
    ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
            "flag",
            Types.BOOLEAN);
    flagState = getRuntimeContext().getState(flagDescriptor);

    ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
            "timer-state",
            Types.LONG);
    timerState = getRuntimeContext().getState(timerDescriptor);
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }
```
{{< /tab >}}
{{< /tabs >}}

`KeyedProcessFunction#processElement` is called with a `Context` that contains a timer service.
The timer service can be used to query the current time, register timers, and delete timers.
With this, you can set a timer for 1 minute in the future every time the flag is set and store the timestamp in `timerState`.

{{< tabs "timer-v2" >}}
{{< tab "Java" >}}
```java
if (transaction.getAmount() < SMALL_AMOUNT) {
    // set the flag to true
    flagState.update(true);

    // set the timer and timer state
    long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
    context.timerService().registerProcessingTimeTimer(timer);
    timerState.update(timer);
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
  // set the flag to true
  flagState.update(true)

  // set the timer and timer state
  val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE
  context.timerService.registerProcessingTimeTimer(timer)
  timerState.update(timer)
}
```
{{< /tab >}}
{{< /tabs >}}

Processing time is wall clock time, and is determined by the system clock of the machine running the operator. 

When a timer fires, it calls `KeyedProcessFunction#onTimer`. 
Overriding this method is how you can implement your callback to reset the flag.

{{< tabs "timerclear-v2" >}}
{{< tab "Java" >}}
```java
public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
    // remove flag after 1 minute
    timerState.clear();
    flagState.clear();
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
override def onTimer(
    timestamp: Long,
    ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
    out: Collector[Alert]): Unit = {
  // remove flag after 1 minute
  timerState.clear()
  flagState.clear()
}
```
{{< /tab >}}
{{< /tabs >}}

Finally, to cancel the timer, you need to delete the registered timer and delete the timer state.
You can wrap this in a helper method and call this method instead of `flagState.clear()`.

{{< tabs "cleanup-v2" >}}
{{< tab "Java" >}}
```java
private void cleanUp(Context ctx) throws Exception {
    // delete timer
    Long timer = timerState.value();
    ctx.timerService().deleteProcessingTimeTimer(timer);

    // clean up all state
    timerState.clear();
    flagState.clear();
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
@throws[Exception]
private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
  // delete timer
  val timer = timerState.value
  ctx.timerService.deleteProcessingTimeTimer(timer)

  // clean up all states
  timerState.clear()
  flagState.clear()
}
```
{{< /tab >}}
{{< /tabs >}}

And that's it, a fully functional, stateful, distributed streaming application!

## Final Application

{{< tabs "finalapplication" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        // Check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            // Clean up our state
            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
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

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  override def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {

    // Get the current state for the current key
    val lastTransactionWasSmall = flagState.value

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        // Output an alert downstream
        val alert = new Alert
        alert.setId(transaction.getAccountId)

        collector.collect(alert)
      }
      // Clean up our state
      cleanUp(context)
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)
      val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE

      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
      out: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }
}
```
{{< /tab >}}
{{< /tabs >}}

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

### Running in an IDE

Running the project in an IDE may result in a `java.lang.NoClassDefFoundError` exception. This is probably because you do not have all required Flink dependencies implicitly loaded into the classpath.

* IntelliJ IDEA: Go to Run > Edit Configurations > Modify options > Select `include dependencies with "Provided" scope`. This run configuration will now include all required classes to run the application from within the IDE.