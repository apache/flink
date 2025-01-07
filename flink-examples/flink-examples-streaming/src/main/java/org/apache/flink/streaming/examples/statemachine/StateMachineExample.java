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

package org.apache.flink.streaming.examples.statemachine;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.examples.statemachine.dfa.State;
import org.apache.flink.streaming.examples.statemachine.event.Alert;
import org.apache.flink.streaming.examples.statemachine.event.Event;
import org.apache.flink.streaming.examples.statemachine.generator.EventsGeneratorFunction;
import org.apache.flink.streaming.examples.statemachine.kafka.EventDeSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

import java.time.Duration;

/**
 * Main class of the state machine example. This class implements the streaming application that
 * receives the stream of events and evaluates a state machine (per originating address) to validate
 * that the events follow the state machine's rules.
 */
public class StateMachineExample {

    /**
     * Main entry point for the program.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) throws Exception {

        // ---- print some usage help ----

        System.out.println(
                "Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms> | --rps <records-per-second>]");
        System.out.println(
                "Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]");
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--backend <hashmap|rocks>]");
        System.out.println("\t[--checkpoint-dir <filepath>]");
        System.out.println("\t[--incremental-checkpoints <true|false>]");
        System.out.println("\t[--output <filepath> OR null for stdout]");
        System.out.println();

        // ---- determine whether to use the built-in source, or read from Kafka ----

        final DataStream<Event> events;
        final ParameterTool params = ParameterTool.fromArgs(args);

        // create the environment to create streams and configure execution
        Configuration configuration = new Configuration();

        final String stateBackend = params.get("backend", "memory");
        if ("hashmap".equals(stateBackend)) {
            final String checkpointDir = params.get("checkpoint-dir");
            configuration.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        } else if ("rocks".equals(stateBackend)) {
            final String checkpointDir = params.get("checkpoint-dir");
            boolean incrementalCheckpoints = params.getBoolean("incremental-checkpoints", false);
            configuration.set(
                    StateBackendOptions.STATE_BACKEND,
                    "org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackendFactory");
            configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incrementalCheckpoints);
            configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        }
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(2000L);

        if (params.has("kafka-topic")) {
            // set up the Kafka reader
            String kafkaTopic = params.get("kafka-topic");
            String brokers = params.get("brokers", "localhost:9092");

            System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);
            System.out.println();

            KafkaSource<Event> source =
                    KafkaSource.<Event>builder()
                            .setBootstrapServers(brokers)
                            .setGroupId("stateMachineExample")
                            .setTopics(kafkaTopic)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            new EventDeSerializationSchema()))
                            .setStartingOffsets(OffsetsInitializer.latest())
                            .build();
            events =
                    env.fromSource(
                            source, WatermarkStrategy.noWatermarks(), "StateMachineExampleSource");
        } else {
            final double errorRate = params.getDouble("error-rate", 0.0);
            final int sleep = params.getInt("sleep", 1);
            final double recordsPerSecond =
                    params.getDouble("rps", rpsFromSleep(sleep, env.getParallelism()));
            System.out.printf(
                    "Using standalone source with error rate %f and %.1f records per second\n",
                    errorRate, recordsPerSecond);
            System.out.println();

            GeneratorFunction<Long, Event> generatorFunction =
                    new EventsGeneratorFunction(errorRate);
            DataGeneratorSource<Event> eventGeneratorSource =
                    new DataGeneratorSource<>(
                            generatorFunction,
                            Long.MAX_VALUE,
                            RateLimiterStrategy.perSecond(recordsPerSecond),
                            TypeInformation.of(Event.class));

            events =
                    env.fromSource(
                            eventGeneratorSource,
                            WatermarkStrategy.noWatermarks(),
                            "Events Generator Source");
        }

        // ---- main program ----

        final String outputFile = params.get("output");

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Alert> alerts =
                events
                        // partition on the address to make sure equal addresses
                        // end up in the same state machine flatMap function
                        .keyBy(Event::sourceAddress)

                        // the function that evaluates the state machine over the sequence of events
                        .flatMap(new StateMachineMapper());

        // output the alerts to std-out
        if (outputFile == null) {
            alerts.print();
        } else {
            alerts.sinkTo(
                            FileSink.<Alert>forRowFormat(
                                            new Path(outputFile), new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .setParallelism(1)
                    .name("output");
        }

        // trigger program execution
        env.execute("State machine job");
    }

    // ------------------------------------------------------------------------

    /**
     * The function that maintains the per-IP-address state machines and verifies that the events
     * are consistent with the current state of the state machine. If the event is not consistent
     * with the current state, the function produces an alert.
     */
    @SuppressWarnings("serial")
    static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {

        /** The state for the current key. */
        private ValueState<State> currentState;

        @Override
        public void open(OpenContext openContext) {
            // get access to the state object
            currentState =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
        }

        @Override
        public void flatMap(Event evt, Collector<Alert> out) throws Exception {
            // get the current state for the key (source address)
            // if no state exists, yet, the state must be the state machine's initial state
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // ask the state machine what state we should go to based on the given event
            State nextState = state.transition(evt.type());

            if (nextState == State.InvalidTransition) {
                // the current event resulted in an invalid transition
                // raise an alert!
                out.collect(new Alert(evt.sourceAddress(), state, evt.type()));
            } else if (nextState.isTerminal()) {
                // we reached a terminal state, clean up the current state
                currentState.clear();
            } else {
                // remember the new state
                currentState.update(nextState);
            }
        }
    }

    // Used for backwards compatibility to convert legacy 'sleep' parameter to records per second.
    private static double rpsFromSleep(int sleep, int parallelism) {
        return (1000d / sleep) * parallelism;
    }
}
