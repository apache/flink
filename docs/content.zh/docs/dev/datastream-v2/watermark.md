---
title: Watermark
weight: 11
type: docs
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

{{< hint warning >}}
**Note:** DataStream API V2 is a new set of APIs, to gradually replace the original DataStream API. It is currently in the experimental stage and is not fully available for production.
{{< /hint >}}

# Watermark

Before introducing Watermark, users should be aware that Watermark in DataStream V2 does not 
refer to the [original Watermark]({{< ref "docs/concepts/time" >}}#event-time-and-watermarks) that
measure progress in event time, but is a special event that can be customized by the user and 
can be propagated along the streams.

Using Watermark in Flink involves three key steps:
1. Define and Declare the Watermark
2. Emit the Watermark
3. Handle the Watermark

Let's follow these steps to understand how to use Watermark in Flink.

## Define and Declare Watermark

A watermark is a special event that carries data. It can be emitted from a `Source` 
or `ProcessFunction`, propagating along streams, received by downstream `ProcessFunction`. 
When defining a Watermark, there are four aspects need to consider:

1. [Required] Watermark Identifier
    
   Since multiple types of Watermarks can propagate in streams, it's essential to assign an 
identifier to each Watermark for differentiation. 

    Note that the identifier must be a String, is case-sensitive, and must be globally unique within the entire job.

2. [Required] Watermark Data Type

   It is important to specify the data type of the Watermark. Currently, Flink supports two types: Long and Bool.

3. [Required] Combine Function and `combineWaitForAllChannels`

   A `ProcessFunction` may receive multiple Watermarks from different input channels due to 
having multiple upstream inputs, each potentially with varying degrees of parallelism. 
In such cases, users often wish to combine Watermarks from input channels before outputting 
to the `ProcessFunction`.

   Flink supports the following combination functions:
    - For `Long` type Watermark:
      - `MIN`: Retains and outputs the minimum value of all received watermarks.
      - `MAX`: Retains and outputs the maximum value of all received watermarks.
    - For `Bool` type Watermark:
      - `AND`: Retains and outputs the logical AND result of all received watermarks.
      - `OR`: Retains and outputs the logical OR result of all received watermarks.

   Additionally, users can configure whether the combining process should wait until the 
`ProcessFunction` has received Watermarks from all upstream channels. This is particularly useful 
in some scenarios. For example, the event time watermark need to wait receives the watermarks from 
all inputs and then combine them. This ensures that the time carried by the event time watermark does not decrease. 
By default, the `combineWaitForAllChannels` setting is false.
    
4. [Optional] `WatermarkHandlingStrategy` by Framework

    The `WatermarkHandlingStrategy` determines whether the framework should send the watermark 
to downstream `ProcessFunction` when the user-defined `ProcessFunction` does pop the Watermark. 
There are two options of this setting:
   - IGNORE: The framework shouldn't take any action.
   - FORWARD: The framework should send the watermark to downstream.

   This optional setting is useful in some cases. For example, setting it to `IGNORE` can indicate 
that the framework does not need to propagate this Watermark, but rather that it is up to the user 
to control its sending.

To simplify the process of defining a Watermark, Flink offer a `WatermarkBuilder`.
This builder ultimately creates a `WatermarkDeclaration` object.
Below is an example demonstrating how to use it to defining a Watermark:

```java
LongWatermarkDeclaration watermarkDeclaration = WatermarkDeclarations
    .newBuilder("MY_CUSTOM_WATERMARK_IDENTIFIER")
    .typeLong()
    .combineFunctionMax()
    .combineWaitForAllChannels(true)
    .defaultHandlingStrategyForward()
    .build();
```

Once users have defined the Watermark, it is essential to declare it in the 
`ProcessFunction#declareWatermarks` or `Source#declareWatermarks`. This step allows the framework to recognize it properly. 
Here’s an example of how to declare the Watermark in the `ProcessFunction`:

```java
public class CustomProcessFunction
        implements OneInputStreamProcessFunction<Long, Long> {

    LongWatermarkDeclaration watermarkDeclaration = ...;

    @Override
    public Set<? extends WatermarkDeclaration> declareWatermarks() {
        return Set.of(watermarkDeclaration);
    }
}
```

Please note that each type of Watermark only needs to be declared once in a job.

## Emit Watermark

Users can utilize the Watermark declaration to create a watermark. Below is an example of how to generate a Long type watermark with a value of 1:

```java
LongWatermarkDeclaration watermarkDeclaration = ...;
LongWatermark watermark = watermarkDeclaration.newWatermark(1);
```

And users can emit Watermark in `ProcessFunction` by calling `nonPartitionedContext.getWatermarkManager().emitWatermark(watermark)`, 
it is also support emit Watermark in `Source` by calling `sourceReaderContext.emitWatermark(watermark)`.
Here is an example of how to emit a Watermark in a `ProcessFunction`:
```java
public class CustomProcessFunction
        implements OneInputStreamProcessFunction<Long, Long> {

    LongWatermarkDeclaration watermarkDeclaration = ...;

    @Override
    public Set<? extends WatermarkDeclaration> declareWatermarks() {
        return Set.of(watermarkDeclaration);
    }

    @Override
    public void processRecord(Long record, Collector<Long> output, PartitionedContext<Long> ctx)
            throws Exception {
        // do something as needed
        
        // generate and emit Watermark
        LongWatermark watermark = watermarkDeclaration.newWatermark(1);
        ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
    }
}
```

## Handle Watermark

Once the `ProcessFunction` receives a Watermark, the framework will invoke the 
`ProcessFunction#onWatermark` method to process it. Therefore, users will need to override 
`ProcessFunction#onWatermark` in order to handle the Watermark appropriately.

The return value of the `ProcessFunction#onWatermark` method is of type `WatermarkHandlingResult`, 
which has two possible options:

- PEEK: `ProcessFunction` only peek the Watermark, and it's framework's responsibility to handle this watermark. 

    The framework will forward/ignore the Watermark according to the `WatermarkHandlingStrategy` associated with it.

- POLL: This Watermark should be sent to downstream by process function itself. The framework does no additional processing.

By default, the `ProcessFunction#onWatermark` method returns `WatermarkHandlingResult.PEEK`.

Here is an example of how to handle Watermarks in `ProcessFunction`:

```java

public static final String CUSTOM_WATERMARK_IDENTIFIER = "CUSTOM_WATERMARK_IDENTIFIER";

public class CustomProcessFunction
        implements OneInputStreamProcessFunction<Long, Long> {
    
    ...

    @Override
    public WatermarkHandlingResult onWatermark(
            Watermark watermark, Collector<Long> output, NonPartitionedContext<Long> ctx) {
        // For Watermark that this ProcessFunction is interested in, process the watermark
        if (watermark.getIdentifier().equals(CUSTOM_WATERMARK_IDENTIFIER)) {
            // do something as needed
        }

        // For other Watermarks, return PEEK
        return WatermarkHandlingResult.PEEK;
    }
}
```

{{< top >}}
