---
title: "DataStream API"
nav-id: datastreamwalkthrough
nav-title: 'DataStream API'
nav-parent_id: walkthroughs
nav-pos: 2
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

Apache Flink offers a DataStream API for building robust, stateful streaming applications.
It provides fine-grained control over state and time, which allows for the implementation of complex event-driven systems.

* This will be replaced by the TOC
{:toc}

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

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) is consistently ranked as one of the most active of any Apache project and a great way to get help quickly.

## How To Follow Along

If you want to follow along, you will require a computer with:

* Java 8 
* Maven 

A provided Flink Maven Archetype will create a skeleton project with all the necessary dependencies quickly:

{% panel **Note:** Each code block within this walkthrough may not contain the full surrounding class for brevity. The full code is available [at the bottom of the page](#final-application). %}

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight bash %}
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \{% unless site.is_stable %}
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
    -DarchetypeVersion={{ site.version }} \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight bash %}
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-scala \{% unless site.is_stable %}
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
    -DarchetypeVersion={{ site.version }} \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
{% endhighlight %}
</div>
</div>

{% unless site.is_stable %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the commandline. If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven official document</a>
</p>
{% endunless %}

You can edit the `groupId`, `artifactId` and `package` if you like. With the above parameters,
Maven will create a project with all the dependencies to complete this tutorial.
After importing the project into your editor, you will see a file with the following code which you can run directly inside your IDE.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
#### FraudDetectionJob.java

{% highlight java %}
package frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            .name("transactions");
        
        DataStream<Alerts> alerts = transactions
            .keyBy(Transaction::getAccountId)
            .process(new FraudDetector())
            .name("fraud-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
{% endhighlight %}

#### FraudDetector.java
{% highlight java %}
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

    public static final double SMALL_AMOUNT = 1.00;

    public static final double LARGE_AMOUNT = 500.00;

    public static final long ONE_DAY = 24 * 60 * 60 * 1000;

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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
#### FraudDetectionJob.scala

{% highlight scala %}
package frauddetection

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.scala.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val transactions = env
            .addSource(new TransactionSource)
            .name("transactions")
        
        val alerts = transactions
            .keyBy(transaction => transaction.getAccountId)
            .process(new FraudDetector)
            .name("fraud-detector")

        alerts
            .addSink(new AlertSink)
            .name("send-alerts")

        env.execute("Fraud Detection")
    }
}
{% endhighlight %}

#### FraudDetector.scala

{% highlight scala %}
object FraudDetector {

    val SMALL_AMOUNT = 1.00

    val LARGE_AMOUNT = 500.00

    val ONE_DAY = 24 * 60 * 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

    override def processElement(
        transaction: Transaction,
        context: Context,
        collector: Collector[Alert]): Unit = {

        Alert alert = new Alert
        alert.setId(transaction.getAccountId)

        collector.collect(alert)
    }
}
{% endhighlight %}
</div>
</div>

## Breaking Down The Code

#### The Execution Environment

The first line sets up your `StreamExecutionEnvironment`.
The execution environment is how you can set properties for your Job and create your sources.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
{% endhighlight %}
</div>
</div>

#### Creating A Source

Sources define connections to external systems that Flink can use to consume data from, for example Apache Kafka, Rabbit MQ, or Apache Pulsar.
This walkthrough uses a source that generates an infinite stream of credit card transactions for you to process.
Each transaction contains an account ID (`accountId`), timestamp (`timestamp`) of when the transaction occurred, and US$ amount (`amount`).
The `name` attached to the source is just for debugging purposes, so if something goes wrong, we will know where the error originated.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Transaction> transactions = env
    .addSource(new TransactionSource())
    .name("transactions")
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val transactions = env
    .addSource(new TransactionSource)
    .name("transactions")
{% endhighlight %}
</div>
</div>


#### Partitioning Events & Detecting Fraud

The stream contains transactions from a large number of users; however, fraud occurs on a per-account basis. To detect fraud, you must ensure that the same instance of the fraud detector processes every event for a given account.

Streams can be partitioned using `DataStream#keyBy` to ensure that the same physical operator processes all records for a particular key.
It is common to say the operator immediatly after a `keyBy` is executed within a _keyed context_.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Alerts> alerts = transactions
    .keyBy(Transaction::getAccountId)
    .process(new FraudDetector())
    .name("fraud-detector");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val alerts = transactions
    .keyBy(transaction => transaction.getAccountId)
    .process(new FraudDetector)
    .name("fraud-detector")
{% endhighlight %}
</div>
</div>

#### Outputting Results
 
Sink's connect Flink Jobs to external systems to send events to; such as Apache Kafka, Casandra, and AWS Kinesis.
The `AlertSink` logs each alert with log level **INFO**, instead of writing to persistent storage, so you can easily see your results.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
alerts.addSink(new AlertSink());
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
alerts.addSink(new AlertSink)
{% endhighlight %}
</div>
</div>

#### Executing The Job

Flink applications are built lazily and shipped to the cluster for execution only once fully formed.
You can call `StreamExecutionEnvironment#execute` to begin the execution of our Job and give it a name.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
env.execute("Fraud Detection");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
env.execute("Fraud Detection")
{% endhighlight %}
</div>
</div>

#### The Fraud Detector

The logic for the fraud detector is encapsulated within a `KeyedProcessFunction`.
This first version produces an alert on every transaction, which some may say is overly conservative.
It also includes several constants that you may find helpful as you work through your implementation.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {
	
	private static final long serialVersionUID = 1L;

    public static final double SMALL_AMOUNT = 1.00;

    public static final double LARGE_AMOUNT = 500.00;

    public static final long ONE_DAY = 24 * 60 * 60 * 1000;

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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
object FraudDetector {

    val SMALL_AMOUNT = 1.00

    val LARGE_AMOUNT = 500.00

    val ONE_DAY = 24 * 60 * 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

    override def processElement(
        transaction: Transaction,
        context: Context,
        collector: Collector[Alert]): Unit = {

        Alert alert = new Alert
        alert.setId(transaction.getAccountId)

        collector.collect(alert)
    }
}
{% endhighlight %}
</div>
</div>

## Writing A Real Application (v1)

For the first version, the fraud detector should output an alert for any account that makes a small transaction immediately followed by a large one. Where small is anything less than $1.00 and large is more than $500.
Imagine your fraud detector processes the following stream of transactions for a particular account.

<p class="text-center">
    <img alt="Transactions" width="80%" src="{{ site.baseurl }}/fig/fraud-transactions.svg"/>
</p>

Transactions 3 and 4 should be marked as fraudulent because it is a small transaction, $0.09, followed by a large one, $510.
Alternatively, transactions 7, 8, and 9 are not fraud because the small amount of $0.02 is not immediately followed by the large one; instead, there is an intermediate transaction that breaks the pattern.

To do this, the fraud detector must _remember_ information across events; a large transaction is only fraudulent if the previous one was small.
Remembering information across events requires [state]({{ site.baseurl }}/concepts/glossary.html#managed-state) and we decided to use a [KeyedProcessFunction]({{ site.baseurl }}/dev/stream/operators/process_function.html), which provides fine-grained control over both state and time.

The most straightforward implementation would be to set a flag whenever a small transaction is processed.
This way, when a large transaction comes through, you can check if the flag is set for that account.
If it is, then this is fraud and output an alert.
This flag is what you want to store in Flink state.

The most basic type of state in Flink is [ValueState]({{ site.baseurl }}/dev/stream/state/state.html#using-managed-keyed-state), a data type that adds fault tolerance to any variable it wraps.
`ValueState` is a form of _keyed state_, meaning it is only available within a _keyed context_; any operator immediately following `DataStream#keyBy`.
Any operations that read or write to _keyed state_ are automatically scoped to the current key.
In this example, the key is the account id for the current transaction, and each account will maintain its own independent state. 
`ValueState` is created using a `ValueStateDescriptor` which contains metadata about how the Flink runtime should manage the variable.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private transient ValueState<Boolean> flagState;

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);

		flagState = getRuntimeContext().getState(flagDescriptor);
	}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

    @transient private var flagState: ValueState[java.lang.Boolean] = _

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
        val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
        flagState = getRuntimeContext.getState(flagDescriptor)
    }
{% endhighlight %}
</div>
</div>

`ValueState` is a wrapper class, similar to `AtomicReference` in the Java standard library.
It provides three methods for interacting with its contents; `update` sets the state, `value` gets the current value, and `clear` to delete its contents.
If the state for a particular key is empty, such as at the beginning of an application or after calling `ValueState#clear`, then `ValueState#update` will return `null`.
Otherwise, fault tolerance is managed automatically under the hood, and so you can interact with it like with any standard variable.

Below, you can see an example of how you can use a flag state to track potential fraudulent transactions.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
            flagState.clear();
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
    override def processElement(
        transaction: Transaction,
        context: Context,
        collector: Collector[Alert]): Unit = {

        // Get the current state for the current key
        val lastTransactionWasSmall = flagState.value

        // Check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                Alert alert = new Alert
                alert.setId(transaction.getAccountId)

                collector.collect(alert)
            }

            // Clean up our state
            flagState.clear()
        }
        


        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true)
        }
    }
}
{% endhighlight %}
</div>
</div>

For every transaction, the fraud detector checks the state of the flag for that account.
Remember, `ValueState` is always scoped to the current key.
If the flag is non-null, then the last transaction seen for that key was small, and so if the amount for this transaction is large, then the detector outputs a fraud alert.

After that check, the flag state is unconditionally cleared.
Either the current transaction caused a fraud alert, and the pattern is over, or the current transaction did not cause an alert, and the pattern is broken and needs to be restarted.

Finally, the transaction amount is checked to see if it is small.
If so, then the flag is set so that it can be checked by the next event.

## Fraud Detector v2: State + Time = &#10084;&#65039;

Many event-driven applications require a strong notion of time to complement their state.
For example, suppose you wanted to set a 24-hour timeout to your fraud detector; i.e., in the previous example transactions 3 and 4 would only be considered fraud if they occurred within 24 hours of each other.
Flink allows setting timers which serve as callbacks at some point in time in the future.

Along with the previous requirements:

* Whenever the flag is set to true, also set a timer for 24 hours in the future.
* When the timer fires, reset the flag by clearing its state.
* If the flag is ever cleared the timer should be canceled.

To cancel a timer, you have to remember what time it is set for, and remembering implies state, so you will begin by creating a timer state along with your flag state.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private transient ValueState<Boolean> flagState;

	private transient ValueState<Long> timerState;

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);

		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);

		timerState = getRuntimeContext().getState(timerDescriptor);
	}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
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
{% endhighlight %}
</div>
</div>

`KeyedProcessFunction#processElement` is called with a `Context` that contains a timer service.
The timer service can be used to query the current time, register timers, and delete timers.
With this, you can set a timer for 24 hours in the future every time the flag is set and store the timestamp in `timerState`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
if (transaction.getAmount() < SMALL_AMOUNT) {
    // set the flag to true
    flagState.update(true);

    long timer = context.timerService().currentProcessingTime() + ONE_DAY;
    context.timerService().registerProcessingTimeTimer(timer);

    timerState.update(timer);
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
    // set the flag to true
    flagState.update(true)
    val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_DAY

    context.timerService.registerProcessingTimeTimer(timer)
    timerState.update(timer)
}
{% endhighlight %}
</div>
</div>

Processing time is wall clock time, and is determined by the system clock of the machine running the operator.

When a timer fires, it calls `KeyedProcessFunction#onTimer`. 
Overriding this method is how you can implement your callback to reset the flag.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
    timerState.clear();
    flagState.clear();
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[Alert]): Unit = {
    timerState.clear()
    flagState.clear()
}
{% endhighlight %}
</div>
</div>

Finally, to cancel the timer, you need to delete the registered timer and delete the timer state.
You can wrap this in a helper method that's called every time a new element is processed.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
private void cleanUp(Context ctx) throws Exception {
    Long timer = timerState.value();

    if (timer != null) {
        ctx.timerService().deleteProcessingTimeTimer(timer);
        timerState.clear();
    }

    flagState.clear();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
@throws[Exception]
private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    val timer = timerState.value

    if (timer != null) {
        ctx.timerService.deleteProcessingTimeTimer(timer)
        timerState.clear()
    }

    flagState.clear()
}
{% endhighlight %}
</div>
</div>

And that's it, a fully functional, stateful, distributed streaming application!

## Final Application

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
package frauddetector;

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

	public static final double SMALL_AMOUNT = 1.00;

	public static final double LARGE_AMOUNT = 500.00;

	public static final long ONE_DAY = 24 * 60 * 60 * 1000;

	private transient ValueState<Boolean> flagState;

	private transient ValueState<Long> timerState;

	@Override
	public void open(Configuration parameters) throws Exception {
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

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }

            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_DAY;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        Long timer = timerState.value();

        if (timer != null) {
            ctx.timerService().deleteProcessingTimeTimer(timer);
            timerState.clear();
        }

        flagState.clear();
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
package frauddetector

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}

object FraudDetector {

    val SMALL_AMOUNT = 1.00

    val LARGE_AMOUNT = 500.00

    val ONE_DAY = 24 * 60 * 60 * 1000L
}

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

    @throws[Exception]
    override def processElement(
        transaction: Transaction,
        context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
        collector: Collector[Alert]): Unit = {

        val lastTransactionWasSmall = flagState.value

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
                val alert = new Alert
                alert.setId(transaction.getAccountId)

                collector.collect(alert)
            }
        
            cleanUp(context)
        }

        if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true)
            val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_DAY

            context.timerService.registerProcessingTimeTimer(timer)
            timerState.update(timer)
        }
    }

    @throws[Exception]
    override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
        out: Collector[Alert]): Unit = {

        timerState.clear()
        flagState.clear()
    }

    @throws[Exception]
    private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
        val timer = timerState.value

        if (timer != null) {
            ctx.timerService.deleteProcessingTimeTimer(timer)
            timerState.clear()
        }

        flagState.clear()
    }
}
{% endhighlight %}
</div>
</div>

### Expected Output

Running this code with the provided `TransactionSource` will emit fraud alerts for account 3.
You should see the following output in your task manager logs: 

{% highlight bash %}
2019-08-19 14:22:06,220 INFO  org.apache.flink.walkthrough.common.sink.AlertSink            - Alert{id=3}
2019-08-19 14:22:11,383 INFO  org.apache.flink.walkthrough.common.sink.AlertSink            - Alert{id=3}
2019-08-19 14:22:16,551 INFO  org.apache.flink.walkthrough.common.sink.AlertSink            - Alert{id=3}
2019-08-19 14:22:21,723 INFO  org.apache.flink.walkthrough.common.sink.AlertSink            - Alert{id=3}
2019-08-19 14:22:26,896 INFO  org.apache.flink.walkthrough.common.sink.AlertSink            - Alert{id=3}
{% endhighlight %}