/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.dsv2.eventtime;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.StateManager;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.function.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.ParameterTool;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * This example illustrates how to count the number of clicks on each news at 1 hour after news
 * publication.
 *
 * <p>The input consists of a series of {@link NewsEvent}s, which fall into two categories: news
 * publish and news click. Each {@link NewsEvent} contains three components: the event type, the
 * news ID and the timestamp. Notably, there is only one event of type {@link NewsEventType#PUBLISH}
 * for each news.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--output &lt;path&gt;</code>The output directory where the Job will write the
 *       results. If no output path is provided, the Job will print the results to <code>stdout
 *       </code>.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Usage of Event Time extension in DataStream API V2
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
public class CountNewsClicks {

    /**
     * The type of {@link NewsEvent}, note that only one event of type {@link NewsEventType#PUBLISH}
     * for each news.
     */
    public enum NewsEventType {
        PUBLISH,
        CLICK
    }

    /**
     * The {@link NewsEvent} represents an event on news, containing the event type, news id and the
     * timestamp.
     */
    public static class NewsEvent {
        public long newsId;
        public long timestamp;
        public NewsEventType type;
    }

    /**
     * The {@link NewsClicks} represents the number of clicks on news within one hour following its
     * publication.
     */
    public static class NewsClicks {
        public long newsId;
        public long clicks;

        public NewsClicks(long newsId, long clicks) {
            this.newsId = newsId;
            this.clicks = clicks;
        }

        @Override
        public String toString() {
            return String.format("%d,%d", this.newsId, this.clicks);
        }
    }

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final boolean fileOutput = params.has("output");

        // obtain execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // the input consists of a series of {code NewsEvent}s, which include two types: news
        // publish event and news click event.
        // We provide a CSV file to represent the sample input data, which can be found in the
        // resources directory.
        String inputFilePath =
                Objects.requireNonNull(
                                CountNewsClicks.class
                                        .getClassLoader()
                                        .getResource(
                                                "datas/dsv2/eventtime/CountNewsClicksEvents.csv"))
                        .getPath();
        // create a FileSource with CSV format to read the input data
        CsvReaderFormat<NewsEvent> csvFormat = CsvReaderFormat.forPojo(NewsEvent.class);
        FileSource<NewsEvent> fileSource =
                FileSource.forRecordStreamFormat(csvFormat, new Path(inputFilePath)).build();
        NonKeyedPartitionStream<NewsEvent> source =
                env.fromSource(new WrappedSource<>(fileSource), "news source");

        NonKeyedPartitionStream<NewsClicks> clicksStream =
                // extract event time and generate the event time watermark
                source.process(
                                // the timestamp field of the input is considered to be the
                                // event time
                                EventTimeExtension.<NewsEvent>newWatermarkGeneratorBuilder(
                                                event -> event.timestamp)
                                        // generate event time watermarks every 200ms
                                        .periodicWatermark(Duration.ofMillis(200))
                                        // if the input is idle for more than 30 seconds, it
                                        // is ignored during the event time watermark
                                        // combination process
                                        .withIdleness(Duration.ofSeconds(30))
                                        // set the maximum out-of-order time of the event to
                                        // 30 seconds, meaning that if an event is received
                                        // at 12:00:00, then no further events should be
                                        // received earlier than 11:59:30
                                        .withMaxOutOfOrderTime(Duration.ofSeconds(10))
                                        // build the event time watermark generator as
                                        // ProcessFunction
                                        .buildAsProcessFunction())
                        // key by the news id
                        .keyBy(event -> event.newsId)
                        // count the click number of each news
                        .process(
                                EventTimeExtension.wrapProcessFunction(
                                        new CountNewsClicksProcessFunction()));

        if (fileOutput) {
            // write results to file
            clicksStream
                    .toSink(
                            new WrappedSink<>(
                                    FileSink.<NewsClicks>forRowFormat(
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
                    .withName("output");
        } else {
            // Print the results to the STDOUT.
            clicksStream.toSink(new WrappedSink<>(new PrintSink<>())).withName("print-sink");
        }

        env.execute("CountNewsClicks");
    }

    /**
     * This process function will consume {@link NewsEvent} and count the number of clicks within 1
     * hour of the news publication and send the results {@link NewsClicks} to the output.
     *
     * <p>To achieve the goal, we will register an event timer for each news, which will be
     * triggered at the time of the news's release time + 1 hour, and record a click count of each
     * news. In the timer callback {@code onEventTimer}, we will output the number of clicks.
     *
     * <p>To handle the potential disorder between news publication event and click events, we will
     * use a ListState to store the timestamps of click events that occur prior to their
     * corresponding publication events.
     *
     * <p>To handle the potential missing news publication event, we will register an event timer
     * for the click event and set the timer to the timestamp of the click event plus one hour.
     */
    public static class CountNewsClicksProcessFunction
            implements OneInputEventTimeStreamProcessFunction<NewsEvent, NewsClicks> {

        private static final long ONE_HOUR_IN_MS = Duration.ofHours(1).toMillis();

        private EventTimeManager eventTimeManager;

        // uses a ValueState to store the publishing time of each news
        private final ValueStateDeclaration<Long> publishTimeStateDeclaration =
                StateDeclarations.valueState("publish_time", TypeDescriptors.LONG);

        // uses a ValueState to store the click count of each news
        private final ValueStateDeclaration<Long> clickCountStateDeclaration =
                StateDeclarations.valueState("click_count", TypeDescriptors.LONG);

        // uses a ListState to store the timestamp of pending clicks for each news.
        // If a click {@code NewsEvent} occurs before the corresponding publication {@link
        // NewsEvent}, we cannot process this click event because we do not know the publication
        // time of the news.
        // Therefore, we store the timestamp of this pending {@code NewsEvent} click in a list
        // state.
        // Once the publication {@link NewsEvent} arrives, we can proceed to process the pending
        // clicks.
        private final ListStateDeclaration<Long> pendingClicksStateDeclaration =
                StateDeclarations.listState("pending_clicks", TypeDescriptors.LONG);

        @Override
        public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
            this.eventTimeManager = eventTimeManager;
        }

        @Override
        public Set<StateDeclaration> usesStates() {
            return Set.of(
                    publishTimeStateDeclaration,
                    clickCountStateDeclaration,
                    pendingClicksStateDeclaration);
        }

        @Override
        public void processRecord(
                NewsEvent record, Collector<NewsClicks> output, PartitionedContext<NewsClicks> ctx)
                throws Exception {

            if (record.type == NewsEventType.PUBLISH) {
                // for the news publication event
                long publishTime = record.timestamp;
                if (eventTimeManager.currentTime() > publishTime + ONE_HOUR_IN_MS) {
                    // if the current event time is more than one hour after the publication time,
                    // it indicates that the publication event is late; therefore, this news should
                    // be ignored.
                    return;
                }

                // 1. record the publishing time
                ctx.getStateManager().getState(publishTimeStateDeclaration).update(publishTime);

                // 2. process pending clicks
                Iterable<Long> pendingClicks =
                        ctx.getStateManager().getState(pendingClicksStateDeclaration).get();
                if (pendingClicks != null) {
                    long addition = 0;
                    for (Long clickTime : pendingClicks) {
                        if (clickTime <= publishTime + ONE_HOUR_IN_MS) {
                            addition++;
                        }
                    }
                    addClickCount(ctx.getStateManager(), addition);
                }

                // 3. register timer with the publishing time + 1 hour
                eventTimeManager.registerTimer(publishTime + ONE_HOUR_IN_MS);
            } else {
                // for the news click event
                Long publishTime =
                        ctx.getStateManager().getState(publishTimeStateDeclaration).value();
                if (publishTime == null) {
                    // If the news publication event has not yet arrived
                    // 1. record the click timestamp in the pending clicks list.
                    ctx.getStateManager()
                            .getState(pendingClicksStateDeclaration)
                            .add(record.timestamp);
                    // 2. to avoid the news not being processed for a long time when there is no
                    // publish event, register timer with the click timestamp + 1 hour.
                    eventTimeManager.registerTimer(record.timestamp + ONE_HOUR_IN_MS);
                } else if (record.timestamp <= publishTime + ONE_HOUR_IN_MS) {
                    // If the click event occurs within one hour of the news publication, update the
                    // click count.
                    addClickCount(ctx.getStateManager(), 1);
                }
            }
        }

        @Override
        public void onEventTimer(
                long timestamp, Collector<NewsClicks> output, PartitionedContext<NewsClicks> ctx)
                throws Exception {

            Long publishTime = ctx.getStateManager().getState(publishTimeStateDeclaration).value();

            if (publishTime == null) {
                // If the publishing time is null, it indicates that there is no publishing event
                // for
                // this news, or the news has already been processed; therefore, it should be
                // ignored.
                clearAllState(ctx.getStateManager());
                return;
            }

            // get the news that the current event timer belongs to
            long newsId = ctx.getStateManager().getCurrentKey();
            // get the click count of the news
            Long clickCount = ctx.getStateManager().getState(clickCountStateDeclaration).value();
            clickCount = clickCount == null ? 0 : clickCount;

            // send the result to output
            output.collect(new NewsClicks(newsId, clickCount));

            // clear the state and mark the news as processed
            clearAllState(ctx.getStateManager());
        }

        private void addClickCount(StateManager stateManager, long addition) throws Exception {
            ValueState<Long> clickCountState = stateManager.getState(clickCountStateDeclaration);
            Long previousCount = clickCountState.value();
            long newlyCount = previousCount == null ? addition : previousCount + addition;
            clickCountState.update(newlyCount);
        }

        private void clearAllState(StateManager stateManager) throws Exception {
            stateManager.getState(publishTimeStateDeclaration).clear();
            stateManager.getState(clickCountStateDeclaration).clear();
            stateManager.getState(pendingClicksStateDeclaration).clear();
        }
    }
}
