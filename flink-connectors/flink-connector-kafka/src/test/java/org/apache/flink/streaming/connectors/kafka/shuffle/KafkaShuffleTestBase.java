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

package org.apache.flink.streaming.connectors.kafka.shuffle;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaConsumerTestBase;
import org.apache.flink.streaming.connectors.kafka.KafkaProducerTestBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionAssigner;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;

import org.junit.BeforeClass;

import java.util.Random;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;

/** Base Test Class for KafkaShuffle. */
public class KafkaShuffleTestBase extends KafkaConsumerTestBase {
    static final long INIT_TIMESTAMP = System.currentTimeMillis();

    @BeforeClass
    public static void prepare() throws Exception {
        KafkaProducerTestBase.prepare();
        ((KafkaTestEnvironmentImpl) kafkaServer)
                .setProducerSemantic(FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    static class KafkaSourceFunction
            extends RichParallelSourceFunction<Tuple3<Integer, Long, Integer>> {
        private volatile boolean running = true;
        private final int numElementsPerProducer;
        private final boolean unBounded;

        KafkaSourceFunction(int numElementsPerProducer) {
            this.numElementsPerProducer = numElementsPerProducer;
            this.unBounded = true;
        }

        KafkaSourceFunction(int numElementsPerProducer, boolean unBounded) {
            this.numElementsPerProducer = numElementsPerProducer;
            this.unBounded = unBounded;
        }

        @Override
        public void run(SourceContext<Tuple3<Integer, Long, Integer>> ctx) throws Exception {
            long timestamp = INIT_TIMESTAMP;
            int sourceInstanceId = getRuntimeContext().getIndexOfThisSubtask();
            for (int i = 0; i < numElementsPerProducer && running; i++) {
                ctx.collect(new Tuple3<>(i, timestamp++, sourceInstanceId));
            }

            while (running && unBounded) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    static KeyedStream<Tuple3<Integer, Long, Integer>, Tuple> createKafkaShuffle(
            StreamExecutionEnvironment env,
            String topic,
            int numElementsPerProducer,
            int producerParallelism,
            TimeCharacteristic timeCharacteristic,
            int numberOfPartitions) {
        return createKafkaShuffle(
                env,
                topic,
                numElementsPerProducer,
                producerParallelism,
                timeCharacteristic,
                numberOfPartitions,
                false);
    }

    static KeyedStream<Tuple3<Integer, Long, Integer>, Tuple> createKafkaShuffle(
            StreamExecutionEnvironment env,
            String topic,
            int numElementsPerProducer,
            int producerParallelism,
            TimeCharacteristic timeCharacteristic,
            int numberOfPartitions,
            boolean randomness) {
        DataStream<Tuple3<Integer, Long, Integer>> source =
                env.addSource(new KafkaSourceFunction(numElementsPerProducer))
                        .setParallelism(producerParallelism);
        DataStream<Tuple3<Integer, Long, Integer>> input =
                (timeCharacteristic == EventTime)
                        ? source.assignTimestampsAndWatermarks(new PunctuatedExtractor(randomness))
                                .setParallelism(producerParallelism)
                        : source;

        return FlinkKafkaShuffle.persistentKeyBy(
                input,
                topic,
                producerParallelism,
                numberOfPartitions,
                kafkaServer.getStandardProperties(),
                0);
    }

    static class PunctuatedExtractor
            implements AssignerWithPunctuatedWatermarks<Tuple3<Integer, Long, Integer>> {
        private static final long serialVersionUID = 1L;
        boolean randomness;
        Random rnd = new Random(123);

        PunctuatedExtractor() {
            randomness = false;
        }

        PunctuatedExtractor(boolean randomness) {
            this.randomness = randomness;
        }

        @Override
        public long extractTimestamp(
                Tuple3<Integer, Long, Integer> element, long previousTimestamp) {
            return element.f1;
        }

        @Override
        public Watermark checkAndGetNextWatermark(
                Tuple3<Integer, Long, Integer> lastElement, long extractedTimestamp) {
            long randomValue = randomness ? rnd.nextInt(10) : 0;
            return new Watermark(extractedTimestamp + randomValue);
        }
    }

    static class PartitionValidator
            extends KeyedProcessFunction<
                    Tuple, Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>> {
        private final KeySelector<Tuple3<Integer, Long, Integer>, Tuple> keySelector;
        private final int numberOfPartitions;
        private final String topic;

        private int previousPartition;

        PartitionValidator(
                KeySelector<Tuple3<Integer, Long, Integer>, Tuple> keySelector,
                int numberOfPartitions,
                String topic) {
            this.keySelector = keySelector;
            this.numberOfPartitions = numberOfPartitions;
            this.topic = topic;
            this.previousPartition = -1;
        }

        @Override
        public void processElement(
                Tuple3<Integer, Long, Integer> in,
                Context ctx,
                Collector<Tuple3<Integer, Long, Integer>> out)
                throws Exception {
            int expectedPartition =
                    KeyGroupRangeAssignment.assignKeyToParallelOperator(
                            keySelector.getKey(in), numberOfPartitions, numberOfPartitions);
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            KafkaTopicPartition partition = new KafkaTopicPartition(topic, expectedPartition);

            // This is how Kafka assign partition to subTask;
            boolean rightAssignment =
                    KafkaTopicPartitionAssigner.assign(partition, numberOfPartitions)
                            == indexOfThisSubtask;
            boolean samePartition =
                    (previousPartition == expectedPartition) || (previousPartition == -1);
            previousPartition = expectedPartition;

            if (!(rightAssignment && samePartition)) {
                throw new Exception("Error: Kafka partition assignment error ");
            }
            out.collect(in);
        }
    }

    static class WatermarkValidator
            extends KeyedProcessFunction<
                    Tuple, Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>> {
        private long previousWatermark = Long.MIN_VALUE; // initial watermark get from timeService

        @Override
        public void processElement(
                Tuple3<Integer, Long, Integer> in,
                Context ctx,
                Collector<Tuple3<Integer, Long, Integer>> out)
                throws Exception {

            long watermark = ctx.timerService().currentWatermark();

            // Notice that the timerService might not be updated if no new watermark has been
            // emitted, hence equivalent
            // watermark is allowed, strictly incremental check is done when fetching watermark from
            // KafkaShuffleFetcher.
            if (watermark < previousWatermark) {
                throw new Exception(
                        "Error: watermark should always increase. current watermark : previous watermark ["
                                + watermark
                                + " : "
                                + previousWatermark
                                + "]");
            }
            previousWatermark = watermark;

            out.collect(in);
        }
    }

    static class ElementCountNoLessThanValidator
            implements MapFunction<Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>> {
        private final int totalCount;
        private int counter = 0;

        ElementCountNoLessThanValidator(int totalCount) {
            this.totalCount = totalCount;
        }

        @Override
        public Tuple3<Integer, Long, Integer> map(Tuple3<Integer, Long, Integer> element)
                throws Exception {
            counter++;

            if (counter == totalCount) {
                throw new SuccessException();
            }

            return element;
        }
    }

    static class ElementCountNoMoreThanValidator
            implements MapFunction<Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>> {
        private final int totalCount;
        private int counter = 0;

        ElementCountNoMoreThanValidator(int totalCount) {
            this.totalCount = totalCount;
        }

        @Override
        public Tuple3<Integer, Long, Integer> map(Tuple3<Integer, Long, Integer> element)
                throws Exception {
            counter++;

            if (counter > totalCount) {
                throw new Exception("Error: number of elements more than expected");
            }

            return element;
        }
    }

    String topic(String prefix, TimeCharacteristic timeCharacteristic) {
        return prefix + "_" + timeCharacteristic;
    }
}
