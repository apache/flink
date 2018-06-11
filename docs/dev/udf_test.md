---
title: "User-Defined Function Testing"
nav-parent_id: dev
nav-pos: 40
nav-show_overview: true
nav-id: test-udf
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
If you're using some transform operators whose implemented
functions don't have any return like "FlatMapFunction", or "Co
ProcessFunction" or funtions you define on your own, you may
want to use TestingRuntimeContext to test it.  
{% highlight java %}
/**
 * Initialization
 *
 * @param isStreaming: if set to true, this is used for DataStream Functions.
 *                     if set to false, this is used for DataSet Functions.
 */
TestingRuntimeContext ctx = new TestingRuntimeContext(isStreaming);
{% endhighlight %}
### DataSet Functions
Assume we're using a WordDistinctFlatMap to remove duplicate words for each line in a textbook.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class WordDistinctFlat extends RichFlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        Set<String> wordsList = new HashSet<>(Arrays.asList(value.split(",")));
        for (String word: wordsList) {
            out.collect(word);
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class WordDistinctFlat extends RichFlatMapFunction[String, String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      val wordsList = value.split(",").toSet
      wordsList.foreach(word => {
        out.collect(word)
      })
    }
}
{% endhighlight %}
</div>
</div>
It's easy to find that we can't test it directly like what we usually do in MapFunction because the results are collected
by flink's Collector. Now we use TestingRuntimeContext to test if the logic is right inside the flatMap.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Test
public void testWordDistinctFlat() throws Exception {
    // "false" means that this is not a datastream function.
    TestingRuntimeContext ctx = new TestingRuntimeContext(false);
    WordDistinctFlat flat = new WordDistinctFlat();
    flat.setRuntimeContext(ctx);
    flat.flatMap("Eat,Eat,Walk,Run", ctx.getCollector());
    Assert.assertArrayEquals(ctx.getCollectorOutput().toArray(), new String[]{"Eat", "Walk", "Run"});
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
@Test
def testWordDistinctFlat(): Unit = {
  import scala.collection.JavaConverters._
  val ctx = new TestingRuntimeContext(false)
  val flat = new WordDistinctFlat()
  flat.setRuntimeContext(ctx)
  flat.flatMap("Eat,Eat,Walk,Run", ctx.getCollector())
  Assert.assertEquals(ctx.getCollectorOutput.asScala.toArray.deep, Array("Eat", "Walk", "Run").deep)
}
{% endhighlight %}
</div>
</div>

Then we want to exclude some specific words by broadcasting them, the codes will be like this.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Test
public void testWordDistinctFlat() throws Exception {
    TestingRuntimeContext ctx = new TestingRuntimeContext(false);
    ctx.setBroadcastVariable("exWords", Collections.singletonList("Eat"));
    WordDistinctFlat flat = new WordDistinctFlat();
    flat.setRuntimeContext(ctx);
    flat.flatMap("Eat,Eat,Walk,Run", ctx.getCollector());
    Assert.assertArrayEquals(ctx.getCollectorOutput().toArray(), new String[]{"Walk", "Run"});
}

public static class WordDistinctFlat extends RichFlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        Set<String> wordsList = new HashSet<>(Arrays.asList(value.split(",")));
        List<String> excludeWords = getRuntimeContext().getBroadcastVariable("exWords");
        for (String word: wordsList) {
            if (!excludeWords.contains(word)) {
                out.collect(word);
            }
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
@Test
def testWordDistinctFlat(): Unit = {
  import scala.collection.JavaConverters._
  val ctx = new TestingRuntimeContext(false)
  ctx.setBroadcastVariable("exWords", Collections.singletonList("Eat"))
  val flat = new WordDistinctFlat()
  flat.setRuntimeContext(ctx)
  flat.flatMap("Eat,Eat,Walk,Run", ctx.getCollector())
  Assert.assertEquals(ctx.getCollectorOutput.asScala.toArray.deep, Array("Walk", "Run").deep)
}

class WordDistinctFlat extends RichFlatMapFunction[String, String] {
  override def flatMap(value: String, out: Collector[String]): Unit = {
    val wordsList = value.split(",").toSet
    val exclude = getRuntimeContext.getBroadcastVariable("exWords")
    wordsList.foreach(word => {
      if (!exclude.contains(word)) {
        out.collect(word)
      }
    })
  }
}
{% endhighlight %}
</div>
</div>


### DataStream Functions
For most functions in DataStream, you can easily test them in the same way that you test with DataSet
functions, the difference is the **State** in datastream, which is used very often in user defined functions. And we've 
already offered some simple states to be used in testing. Now we can show you how to use these things
in a real user defined function.  
Let's assume that we're going to build a system for attributing taxis' rides and fare, so what we need is to write an user-defined function
to join rides with fare. The source code of this function is from [JoinRidesWithFares](https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/JoinRidesWithFares.java).
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

static class TaxiRide {
    private long eventTime;

    TaxiRide(long eventTime) { this.eventTime = eventTime; }

    Long getEventTime() { return eventTime; }
}

static class TaxiFare {
    private long eventTime;
    TaxiFare(long eventTime) { this.eventTime = eventTime; }
    Long getEventTime() { return eventTime; }
}

public static class EnrichmentFunction extends CoProcessFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
    // keyed, managed state
    private ValueState<TaxiRide> rideState;
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration config) {
        rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
        fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        if (fareState.value() != null) {
            ctx.output(unmatchedFares, fareState.value());
            fareState.clear();
        }
        if (rideState.value() != null) {
            ctx.output(unmatchedRides, rideState.value());
            rideState.clear();
        }
    }

    @Override
    public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiFare fare = fareState.value();
        if (fare != null) {
            fareState.clear();
            out.collect(new Tuple2(ride, fare));
        } else {
            rideState.update(ride);
            // as soon as the watermark arrives, we can stop waiting for the corresponding fare
            context.timerService().registerEventTimeTimer(ride.getEventTime());
        }
    }

    @Override
    public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiRide ride = rideState.value();
        if (ride != null) {
            rideState.clear();
            out.collect(new Tuple2(ride, fare));
        } else {
            fareState.update(fare);
            // wait up to 6 hours for the corresponding ride END event, then clear the state
            context.timerService().registerEventTimeTimer(fare.getEventTime() + 6 * 60 * 60 * 1000);
        }
    }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val unmatchedRides: OutputTag[TestingRuntimeContextTest.TaxiRide] = new OutputTag[TestingRuntimeContextTest.TaxiRide]("unmatchedRides") {}
val unmatchedFares: OutputTag[TestingRuntimeContextTest.TaxiFare] = new OutputTag[TestingRuntimeContextTest.TaxiFare]("unmatchedFares") {}

class TaxiRide(eventTime: Long) {
  def getEventTime: Long = eventTime
}

class TaxiFare(eventTime: Long) {
  def getEventTime: Long = eventTime
}

class EnrichmentFunction extends CoProcessFunction[TaxiRide, TaxiFare, Tuple2[TaxiRide, TaxiFare]] { // keyed, managed state
  var rideState: ValueState[TaxiRide] = _
  var fareState: ValueState[TaxiFare] = _

  override def open(config: Configuration): Unit = {
    rideState = getRuntimeContext.getState(new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    fareState = getRuntimeContext.getState(new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[TaxiRide, TaxiFare, Tuple2[TaxiRide, TaxiFare]]#OnTimerContext, out: Collector[Tuple2[TaxiRide, TaxiFare]]): Unit = {
    if (fareState.value != null) {
      ctx.output(unmatchedFares, fareState.value)
      fareState.clear()
    }
    if (rideState.value != null) {
      ctx.output(unmatchedRides, rideState.value)
      rideState.clear()
    }
  }

  override def processElement1(ride: TaxiRide, context: CoProcessFunction[TaxiRide, TaxiFare, Tuple2[TaxiRide, TaxiFare]]#Context, out: Collector[Tuple2[TaxiRide, TaxiFare]]): Unit = {
    val fare = fareState.value
    if (fare != null) {
      fareState.clear()
      out.collect(new Tuple2(ride, fare))
    }
    else {
      rideState.update(ride)
      // as soon as the watermark arrives, we can stop waiting for the corresponding fare
      context.timerService.registerEventTimeTimer(ride.getEventTime)
    }
  }

  override def processElement2(fare: TaxiFare, context: CoProcessFunction[TaxiRide, TaxiFare, Tuple2[TaxiRide, TaxiFare]]#Context, out: Collector[Tuple2[TaxiRide, TaxiFare]]): Unit = {
    val ride = rideState.value
    if (ride != null) {
      rideState.clear()
      out.collect(new Tuple2(ride, fare))
    }
    else {
      fareState.update(fare)
      // wait up to 6 hours for the corresponding ride END event, then clear the state
      context.timerService.registerEventTimeTimer(fare.getEventTime + 6 * 60 * 60 * 1000)
    }
  }
}
{% endhighlight %}
</div>
</div>
Let's try some tests for the EnrichmentFunction.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Test
public void testEnrichmentFunction() throws Exception {
	TestingRuntimeContext ctx = new TestingRuntimeContext(true);
	EnrichmentFunction func = new EnrichmentFunction();
	func.setRuntimeContext(ctx);

        // have to mannually mock the context inside the function.
	CoProcessFunction.Context context = mock(EnrichmentFunction.Context.class);
	CoProcessFunction.OnTimerContext timerContext = mock(EnrichmentFunction.OnTimerContext.class);
	TimerService timerService = mock(TimerService.class);
	doAnswer(invocationOnMock -> {
		OutputTag outputTag = invocationOnMock.getArgumentAt(0, OutputTag.class);
		Object value = invocationOnMock.getArgumentAt(1, Object.class);
		ctx.addSideOutput(outputTag, value);
		return null;
	}).when(timerContext).output(any(OutputTag.class), any());
	doReturn(timerService).when(context).timerService();
	doNothing().when(timerService).registerEventTimeTimer(anyLong());

        // use simple states inside "org.apache.flink.api.common.functions.util.test"
	ValueStateDescriptor<TaxiRide> rideStateDesc = new ValueStateDescriptor<>("saved ride", TaxiRide.class);
	ValueStateDescriptor<TaxiFare> fareStateDesc = new ValueStateDescriptor<>("saved fare", TaxiFare.class);
	ctx.setState(rideStateDesc, new SimpleValueState<>(null));
	ctx.setState(fareStateDesc, new SimpleValueState<TaxiFare>(null));
	func.open(new Configuration());

        // receive the first taxi ride.
	TaxiRide ride1 = new TaxiRide(1);
	func.processElement1(ride1, context, ctx.getCollector());
	Assert.assertEquals(ctx.getState(rideStateDesc).value(), ride1);

        // receive the first taxi fare and do the attribution.
	TaxiFare fare1 = new TaxiFare(1);
	func.processElement2(fare1, context, ctx.getCollector());
	Assert.assertEquals(ctx.getState(rideStateDesc).value(), null);
	Assert.assertEquals(ctx.getCollectorOutput(), Collections.singletonList(new Tuple2(ride1, fare1)));
    
        // receive the second taxi fare.
	TaxiFare fare2 = new TaxiFare(2);
	func.processElement2(fare2, context, ctx.getCollector());
	Assert.assertEquals(ctx.getState(fareStateDesc).value(), fare2);

	func.onTimer(0L, timerContext, ctx.getCollector());
	Assert.assertEquals(Collections.singletonList(fare2), ctx.getSideOutput(unmatchedFares));
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
@Test
def testEnrichmentFunction(): Unit = {
  val ctx = new TestingRuntimeContext(true)
  val func = new EnrichmentFunction
  func.setRuntimeContext(ctx)
  
  // have to mannually mock the context inside the function.
  val context = mock(classOf[CoProcessFunction[TaxiRide, TaxiFare, Tuple2[TaxiRide, TaxiFare]]#Context])
  val timerContext = mock(classOf[CoProcessFunction[TaxiRide, TaxiFare, Tuple2[TaxiRide, TaxiFare]]#OnTimerContext])
  val timerService = mock(classOf[TimerService])
  doAnswer(new Answer[Any] {
    override def answer(invocationOnMock: InvocationOnMock) = {
      val outputTag = invocationOnMock.getArgumentAt(0, classOf[OutputTag[Any]])
      val value = invocationOnMock.getArgumentAt(1, classOf[Any])
      ctx.addSideOutput(outputTag, value)
      null
    }
  }).when(timerContext).output(any(classOf[OutputTag[Any]]), any)
  doReturn(timerService).when(context).timerService
  doNothing().when(timerService).registerEventTimeTimer(anyLong)
  
  // use simple states inside "org.apache.flink.api.common.functions.util.test"
  val rideStateDesc = new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide])
  val fareStateDesc = new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare])
  ctx.setState(rideStateDesc, new SimpleValueState[AnyRef](null))
  ctx.setState(fareStateDesc, new SimpleValueState[TaxiFare](null))
  func.open(new Configuration)
  
  // receive the first taxi ride.
  val ride1 = new TaxiRide(1)
  func.processElement1(ride1, context, ctx.getCollector())
  Assert.assertEquals(ctx.getState(rideStateDesc).value, ride1)
  
  // receive the first taxi fare and do the attribution.
  val fare1 = new TaxiFare(1)
  func.processElement2(fare1, context, ctx.getCollector())
  Assert.assertEquals(ctx.getState(rideStateDesc).value, null)
  Assert.assertEquals(ctx.getCollectorOutput, Collections.singletonList(new Tuple2(ride1, fare1)))
  
  // receive the second taxi fare.
  val fare2 = new TaxiFare(2)
  func.processElement2(fare2, context, ctx.getCollector())
  Assert.assertEquals(ctx.getState(fareStateDesc).value, fare2)
  
  func.onTimer(0L, timerContext, ctx.getCollector())
  Assert.assertEquals(Collections.singletonList(fare2), ctx.getSideOutput(unmatchedFares))
}
{% endhighlight %}
</div>
</div>
{% top %}


















