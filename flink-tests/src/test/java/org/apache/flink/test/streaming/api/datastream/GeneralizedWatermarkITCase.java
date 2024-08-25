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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.api.watermark.LongWatermark;
import org.apache.flink.api.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.watermark.WatermarkDeclaration;
import org.apache.flink.api.watermark.WatermarkDeclarations;
import org.apache.flink.api.watermark.WatermarkPolicy;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * This example demonstrate how to detect hot topics on Twitter.
 *
 * <p>The input is a stream of tweets, each has a topic and a posting time. A topic is considered
 * hot if it appears in more than 100 tweets within 5 minutes. To prevent infinite memory footprint
 * increasing, Tweets posted 5 minutes earlier than the newest received tweets will be discarded.
 */
public class GeneralizedWatermarkITCase {

    static final String NEWEST_TWEET_TIME_WM_ID = "newest-tweet-posting-time";
    private NonKeyedPartitionStream<Tweet> nonKeyedPartitionStream;
    private ExecutionEnvironment env;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        env = ExecutionEnvironment.getInstance();
        nonKeyedPartitionStream =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(
                                Arrays.asList(
                                        new Tweet("t1", 1),
                                        new Tweet("t2", 2),
                                        new Tweet("t3", 3))),
                        "test-source");
    }

    static class ExtractTopicAndPostingTimeFunction
            implements OneInputStreamProcessFunction<Tweet, Tuple2<String, Long>> {

        private static final LongWatermarkDeclaration newestTweetTimeWMD =
                WatermarkDeclarations.newBuilder(NEWEST_TWEET_TIME_WM_ID) // ID is always required
                        .typeLong()
                        .combinerMax() // always pick the newest among all parallel upstreams
                        .build();

        private long newestPostingTime = -1L; // use variable instead of state for simplicity

        @Override
        public Collection<WatermarkDeclaration> watermarkDeclarations() {
            return Collections.singleton(newestTweetTimeWMD);
        }

        @Override
        public void processRecord(
                Tweet record,
                org.apache.flink.datastream.api.common.Collector<Tuple2<String, Long>> output,
                PartitionedContext ctx)
                throws Exception {
            String topic = record.getTopic();
            long time = record.getPostingTime();

            // extract the topic and posting time, and forward to downstream
            output.collect(Tuple2.of(topic, time));

            // if the posting time is the newest, emit a new watermark
            if (time > newestPostingTime) {
                newestPostingTime = time;
                ctx.getNonPartitionedContext()
                        .getWatermarkManager()
                        .emitWatermark(
                                // this is similar to instantiate an object out of a class
                                newestTweetTimeWMD.newWatermark(newestPostingTime));
            }
        }
    }

    static class Tweet implements Serializable {
        private static final long serialVersionUID = 1L;

        private String topic;
        private long postingTime;

        public Tweet(String topic, long postingTime) {
            this.topic = topic;
            this.postingTime = postingTime;
        }

        public String getTopic() {
            return topic;
        }

        public long getPostingTime() {
            return postingTime;
        }
    }

    static class DetectHotTopic
            implements OneInputStreamProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

        private static final int HOT_TOPIC_NUM_THRESHOLD = 100;
        private static final long HOT_TOPIC_TIME_THRESHOLD = 5 * 60 * 1000L;
        private Map<String, PriorityQueue<Long>> cachedTweetsByTopic =
                new HashMap<>(); // use variable instead of state for simplicity

        @Override
        public void processRecord(
                Tuple2<String, Long> record,
                org.apache.flink.datastream.api.common.Collector<Tuple2<String, Long>> output,
                PartitionedContext ctx)
                throws Exception {
            String topic = record.f0;
            Long timestamp = record.f1;

            // cache the new received tweet
            PriorityQueue<Long> cachedTweets =
                    cachedTweetsByTopic.computeIfAbsent(topic, (ignore) -> new PriorityQueue<>());
            cachedTweets.add(timestamp);

            // if the topic is hot, output
            if (cachedTweets.size() > HOT_TOPIC_NUM_THRESHOLD) {
                output.collect(
                        Tuple2.of(
                                topic,
                                cachedTweets.peek() // the time that the topic starts to be hot
                                ));
            }
        }

        @Override
        public WatermarkPolicy onWatermark(
                GeneralizedWatermark watermark,
                Collector<Tuple2<String, Long>> output,
                NonPartitionedContext<Tuple2<String, Long>> ctx) {
            // This PF only handles watermarks with the target ID
            if (watermark instanceof LongWatermark
                    && watermark.getIdentifier().equals(NEWEST_TWEET_TIME_WM_ID)) {
                System.out.println(((LongWatermark) watermark).getValue());
                // remove cached tweets 5 minutes earlier than th newest
                cachedTweetsByTopic.forEach(
                        (topic, tweetsInOrder) -> {
                            while (!tweetsInOrder.isEmpty()
                                    && ((LongWatermark) watermark).getValue() - tweetsInOrder.peek()
                                            > HOT_TOPIC_TIME_THRESHOLD) {
                                tweetsInOrder.poll();
                            }
                        });
            }
            return WatermarkPolicy
                    .PEEK; // we don't know if any downstream PF is relying on the watermark
        }
    }

    @Test
    void testCustomGeneralizedWatermarksAreSentAndReceived() throws Exception {
        nonKeyedPartitionStream
                .process(new ExtractTopicAndPostingTimeFunction())
                .keyBy(tuple -> tuple.f0) // by topic
                .process(new DetectHotTopic());
        env.execute("dsv2 job");
    }
}
