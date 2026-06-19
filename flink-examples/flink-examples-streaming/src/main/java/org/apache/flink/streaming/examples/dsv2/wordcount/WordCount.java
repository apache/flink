/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.dsv2.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

/**
 * Implements the "WordCount" program by DataStream API V2 that computes a simple word occurrence
 * histogram over text files. The job will currently be executed in streaming mode, and will support
 * batch mode execution in the future.
 *
 * <p>The input is a [list of] plain text file[s] with lines separated by a newline character.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--input &lt;path&gt;</code>A list of input files and / or directories to read. If no
 *       input is provided, the program is run with default data from {@link WordCountData}.
 *   <li><code>--discovery-interval &lt;duration&gt;</code>Turns the file reader into a continuous
 *       source that will monitor the provided input directories every interval and read any new
 *       files.
 *   <li><code>--output &lt;path&gt;</code>The output directory where the Job will write the
 *       results. If no output path is provided, the Job will print the results to <code>stdout
 *       </code>.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Write a simple Flink program by DataStream API V2
 *   <li>Use tuple data types
 *   <li>Write and use a user-defined process function
 * </ul>
 *
 * <p>Please note that if you intend to run this example in an IDE, you must first add the following
 * VM options: "--add-opens=java.base/java.util=ALL-UNNAMED". This is necessary because the module
 * system in JDK 17+ restricts some reflection operations.
 *
 * <p>Please note that the DataStream API V2 is a new set of APIs, to gradually replace the original
 * DataStream API. It is currently in the experimental stage and is not fully available for
 * production.
 */
public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Get the execution environment instance. This is the main entrypoint
        // to building a Flink application.
        final ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // Apache Flink’s unified approach to stream and batch processing means that a DataStream
        // application executed over bounded input will produce the same final results regardless
        // of the configured execution mode. It is important to note what final means here: a job
        // executing in STREAMING mode might produce incremental updates (think upserts in
        // a database) while in BATCH mode, it would only produce one final result at the end. The
        // final result will be the same if interpreted correctly, but getting there can be
        // different.
        //
        // The “classic” execution behavior of the DataStream API is called STREAMING execution
        // mode. Applications should use streaming execution for unbounded jobs that require
        // continuous incremental processing and are expected to stay online indefinitely.
        //
        // By enabling BATCH execution, we allow Flink to apply additional optimizations that we
        // can only do when we know that our input is bounded. For example, different
        // join/aggregation strategies can be used, in addition to a different shuffle
        // implementation that allows more efficient task scheduling and failure recovery behavior.
        //
        // By setting the runtime mode to AUTOMATIC, Flink will choose BATCH if all sources
        // are bounded and otherwise STREAMING.
        //
        // This job will currently be executed in streaming mode, and will support batch mode
        // execution in the future.
        // TODO Support batch mode execution after the completion of the FLINK-37290 issue.
        env.setExecutionMode(RuntimeExecutionMode.STREAMING);

        NonKeyedPartitionStream<String> text;

        if (params.has("input")) {
            // Create a new file source that will read files from a given set of directories.
            // Each file will be processed as plain text and split based on newlines.
            FileSource.FileSourceBuilder<String> builder =
                    FileSource.forRecordStreamFormat(
                            new TextLineInputFormat(), new Path(params.get("input")));

            // If a discovery interval is provided, the source will
            // continuously watch the given directories for new files.
            if (params.has("discovery-interval")) {
                Duration discoveryInterval =
                        TimeUtils.parseDuration(params.get("discovery-interval"));
                builder.monitorContinuously(discoveryInterval);
            }

            text = env.fromSource(new WrappedSource<>(builder.build()), "file-input");
        } else {
            // Create a new from data source with default data {@code WordCountData}.
            text =
                    env.fromSource(
                            DataStreamV2SourceUtils.fromData(Arrays.asList(WordCountData.WORDS)),
                            "in-memory-input");
        }

        KeyedPartitionStream<String, Tuple2<String, Integer>> keyedStream =
                // The text lines read from the source are split into words
                // using a user-defined process function. The tokenizer, implemented below,
                // will output each word as a (2-tuple) containing (word, 1)
                text.process(new Tokenizer())
                        .withName("tokenizer")
                        // keyBy groups tuples based on the first field, the word.
                        // Using a keyBy allows performing aggregations and other
                        // stateful transformations over data on a per-key basis.
                        // This is similar to a GROUP BY clause in a SQL query.
                        .keyBy(value -> value.f0);

        // For each key, we perform a simple sum of the second field, the count by user-defined
        // Counter process function. It will continuously output updates each time it sees
        // a new instance of each word in the stream.
        // Flink will provide more convenient built-in functions, such as aggregation, in the
        // future. This will
        // allow users to avoid having to write their own Counter process
        // functions in this application.
        NonKeyedPartitionStream<Tuple2<String, Integer>> counts =
                keyedStream.process(new Counter());

        if (params.has("output")) {
            // Given an output directory, Flink will write the results to a file
            // using a simple string encoding. In a production environment, this might
            // be something more structured like CSV, Avro, JSON, or Parquet.
            counts.toSink(
                            new WrappedSink<>(
                                    FileSink.<Tuple2<String, Integer>>forRowFormat(
                                                    new Path(params.get("output")),
                                                    new SimpleStringEncoder<>())
                                            .withRollingPolicy(
                                                    DefaultRollingPolicy.builder()
                                                            .withMaxPartSize(
                                                                    MemorySize.ofMebiBytes(1))
                                                            .withRolloverInterval(
                                                                    Duration.ofSeconds(10))
                                                            .build())
                                            .build()))
                    .withName("file-sink");
        } else {
            // Print the results to the STDOUT.
            counts.toSink(new WrappedSink<>(new PrintSink<>())).withName("print-sink");
        }

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("WordCount");
    }

    // *************************************************************************
    // USER PROCESS FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * ProcessFunction. The process function takes a line (String) and splits it into multiple pairs
     * in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements OneInputStreamProcessFunction<String, Tuple2<String, Integer>> {

        @Override
        public void processRecord(
                String record,
                Collector<Tuple2<String, Integer>> output,
                PartitionedContext<Tuple2<String, Integer>> ctx)
                throws Exception {
            // normalize and split the line
            String[] tokens = record.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    output.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    /**
     * Implements a word counter as a user-defined ProcessFunction that counts received words in
     * streaming mode. The function uses a ValueState to store the count of each word, it will
     * update the count of word and output the result when receive a record "(word,1)".
     *
     * <p>Note that this is just an example of how to code a streaming job using the DataStream API
     * V2. It currently involves some complexity. In the future, we will provide more user-friendly
     * APIs and extensions to simplify the process.
     */
    public static final class Counter
            implements OneInputStreamProcessFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>> {

        // uses a ValueState to store the count of each word
        private final ValueStateDeclaration<Integer> countStateDeclaration =
                StateDeclarations.valueState("count", TypeDescriptors.INT);

        @Override
        public Set<StateDeclaration> usesStates() {
            // declare a ValueState to store the count of each word
            return Set.of(countStateDeclaration);
        }

        @Override
        public void processRecord(
                Tuple2<String, Integer> record,
                Collector<Tuple2<String, Integer>> output,
                PartitionedContext<Tuple2<String, Integer>> ctx)
                throws Exception {
            // calculate the new count of the word
            String word = record.f0;
            Integer count = record.f1;
            Integer previousCount = ctx.getStateManager().getState(countStateDeclaration).value();
            Integer newlyCount = previousCount == null ? count : previousCount + count;

            // update the count of the word
            ctx.getStateManager().getState(countStateDeclaration).update(newlyCount);

            // output the result
            output.collect(Tuple2.of(word, newlyCount));
        }
    }
}
