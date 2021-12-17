/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.windowing.sessionwindows;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** ITCase for Session Windows. */
public class SessionWindowITCase extends AbstractTestBase {

    // seed for the pseudo random engine of this test
    private static final long RANDOM_SEED = 1234567;

    // flag to activate outputs (for debugging)
    private static final boolean OUTPUT_RESULTS_AS_STRING = false;

    // IMPORTANT: this should currently always be set to false
    private static final boolean PURGE_WINDOW_ON_FIRE = false;

    // number of sessions generated in the test (the more, the longer it takes)
    private static final long NUMBER_OF_SESSIONS = 20_000;

    // max. allowed gap between two events of one session
    private static final long MAX_SESSION_EVENT_GAP_MS = 1_000;

    // the allowed lateness after the watermark
    private static final long ALLOWED_LATENESS_MS = 500;

    // maximum additional gap we randomly add between two sessions
    private static final long MAX_ADDITIONAL_SESSION_GAP_MS = 5_000;

    // number of timely events per session
    private static final int EVENTS_PER_SESSION = 10;

    // number of late events per session inside lateness
    private static final int LATE_EVENTS_PER_SESSION = 5;

    // number of late events per session after lateness (will be dropped)
    private static final int MAX_DROPPED_EVENTS_PER_SESSION = 5;

    // number of different session keys
    private static final int NUMBER_OF_DIFFERENT_KEYS = 20;

    // number of parallel in-flight sessions generated in the test stream
    private static final int PARALLEL_SESSIONS = 10;

    // names to address some counters used for result checks
    private static final String SESSION_COUNTER_ON_TIME_KEY = "ALL_SESSIONS_ON_TIME_COUNT";
    private static final String SESSION_COUNTER_LATE_KEY = "ALL_SESSIONS_LATE_COUNT";

    @Test
    public void testSessionWindowing() throws Exception {
        SessionEventGeneratorDataSource dataSource = new SessionEventGeneratorDataSource();
        runTest(dataSource, new ValidatingWindowFunction());
    }

    private void runTest(
            SourceFunction<SessionEvent<Integer, TestEventPayload>> dataSource,
            WindowFunction<SessionEvent<Integer, TestEventPayload>, String, Tuple, TimeWindow>
                    windowFunction)
            throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WindowedStream<SessionEvent<Integer, TestEventPayload>, Tuple, TimeWindow> windowedStream =
                env.addSource(dataSource)
                        .keyBy("sessionKey")
                        .window(
                                EventTimeSessionWindows.withGap(
                                        Time.milliseconds(MAX_SESSION_EVENT_GAP_MS)));

        if (ALLOWED_LATENESS_MS != Long.MAX_VALUE) {
            windowedStream = windowedStream.allowedLateness(Time.milliseconds(ALLOWED_LATENESS_MS));
        }

        if (PURGE_WINDOW_ON_FIRE) {
            windowedStream = windowedStream.trigger(PurgingTrigger.of(EventTimeTrigger.create()));
        }

        windowedStream.apply(windowFunction).print();
        JobExecutionResult result = env.execute();

        // check that overall event counts match with our expectations. remember that late events
        // within lateness will
        // each trigger a window!
        Assert.assertEquals(
                (LATE_EVENTS_PER_SESSION + 1) * NUMBER_OF_SESSIONS * EVENTS_PER_SESSION,
                (long) result.getAccumulatorResult(SESSION_COUNTER_ON_TIME_KEY));
        Assert.assertEquals(
                NUMBER_OF_SESSIONS * (LATE_EVENTS_PER_SESSION * (LATE_EVENTS_PER_SESSION + 1) / 2),
                (long) result.getAccumulatorResult(SESSION_COUNTER_LATE_KEY));
    }

    /** Window function that performs correctness checks for this test case. */
    private static final class ValidatingWindowFunction
            extends RichWindowFunction<
                    SessionEvent<Integer, TestEventPayload>, String, Tuple, TimeWindow> {

        static final long serialVersionUID = 865723993979L;

        @Override
        public void apply(
                Tuple tuple,
                TimeWindow timeWindow,
                Iterable<SessionEvent<Integer, TestEventPayload>> input,
                Collector<String> output)
                throws Exception {

            if (OUTPUT_RESULTS_AS_STRING) {
                output.collect("--- window triggered ---");
            }

            List<SessionEvent<Integer, TestEventPayload>> sessionEvents = new ArrayList<>();

            for (SessionEvent<Integer, TestEventPayload> evt : input) {

                if (OUTPUT_RESULTS_AS_STRING) {
                    output.collect(evt.toString());
                }

                sessionEvents.add(evt);
            }

            // bit-sets to track uniqueness of ids
            BitSet onTimeBits = new BitSet(EVENTS_PER_SESSION);
            BitSet lateWithingBits = new BitSet(LATE_EVENTS_PER_SESSION);

            int onTimeCount = 0;
            int lateCount = 0;

            for (SessionEvent<Integer, TestEventPayload> evt : sessionEvents) {

                if (SessionEventGeneratorImpl.Timing.TIMELY.equals(
                        evt.getEventValue().getTiming())) {

                    ++onTimeCount;
                    onTimeBits.set(evt.getEventValue().getEventId());
                } else if (SessionEventGeneratorImpl.Timing.IN_LATENESS.equals(
                        evt.getEventValue().getTiming())) {

                    ++lateCount;
                    lateWithingBits.set(evt.getEventValue().getEventId() - EVENTS_PER_SESSION);
                } else {

                    Assert.fail("Illegal event type in window " + timeWindow + ": " + evt);
                }
            }

            getRuntimeContext().getLongCounter(SESSION_COUNTER_ON_TIME_KEY).add(onTimeCount);
            getRuntimeContext().getLongCounter(SESSION_COUNTER_LATE_KEY).add(lateCount);

            if (sessionEvents.size() >= EVENTS_PER_SESSION) { // on time events case or non-purging

                // check that the expected amount if events is in the window
                Assert.assertEquals(onTimeCount, EVENTS_PER_SESSION);

                // check that no duplicate events happened
                Assert.assertEquals(onTimeBits.cardinality(), onTimeCount);
                Assert.assertEquals(lateWithingBits.cardinality(), lateCount);
            } else {

                Assert.fail(
                        "Event count for session window "
                                + timeWindow
                                + " is too low: "
                                + sessionEvents);
            }
        }
    }

    /** A data source that is fed from a ParallelSessionsEventGenerator. */
    private static final class SessionEventGeneratorDataSource
            implements SourceFunction<SessionEvent<Integer, TestEventPayload>> {

        static final long serialVersionUID = 11341498979L;

        private volatile boolean isRunning;

        public SessionEventGeneratorDataSource() {
            this.isRunning = false;
        }

        @Override
        public void run(SourceContext<SessionEvent<Integer, TestEventPayload>> ctx) {
            ParallelSessionsEventGenerator<Integer, SessionEvent<Integer, TestEventPayload>>
                    generator = createGenerator();
            this.isRunning = true;
            // main data source driver loop
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    SessionEvent<Integer, TestEventPayload> evt = generator.nextEvent();
                    if (evt != null) {
                        ctx.collectWithTimestamp(evt, evt.getEventTimestamp());
                        ctx.emitWatermark(new Watermark(generator.getWatermark()));
                    } else {
                        break;
                    }
                }
            }
        }

        private ParallelSessionsEventGenerator<Integer, SessionEvent<Integer, TestEventPayload>>
                createGenerator() {
            LongRandomGenerator randomGenerator = new LongRandomGenerator(RANDOM_SEED);

            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < NUMBER_OF_DIFFERENT_KEYS; ++i) {
                keys.add(i);
            }

            GeneratorConfiguration generatorConfiguration =
                    GeneratorConfiguration.of(
                            ALLOWED_LATENESS_MS,
                            LATE_EVENTS_PER_SESSION,
                            MAX_DROPPED_EVENTS_PER_SESSION,
                            MAX_ADDITIONAL_SESSION_GAP_MS);
            GeneratorEventFactory<Integer, SessionEvent<Integer, TestEventPayload>>
                    generatorEventFactory =
                            new GeneratorEventFactory<
                                    Integer, SessionEvent<Integer, TestEventPayload>>() {
                                @Override
                                public SessionEvent<Integer, TestEventPayload> createEvent(
                                        Integer key,
                                        int sessionId,
                                        int eventId,
                                        long eventTimestamp,
                                        long globalWatermark,
                                        SessionEventGeneratorImpl.Timing timing) {
                                    return SessionEvent.of(
                                            key,
                                            TestEventPayload.of(
                                                    globalWatermark, sessionId, eventId, timing),
                                            eventTimestamp);
                                }
                            };

            EventGeneratorFactory<Integer, SessionEvent<Integer, TestEventPayload>>
                    eventGeneratorFactory =
                            new EventGeneratorFactory<>(
                                    generatorConfiguration,
                                    generatorEventFactory,
                                    MAX_SESSION_EVENT_GAP_MS,
                                    EVENTS_PER_SESSION,
                                    randomGenerator);
            return new ParallelSessionsEventGenerator<>(
                    keys,
                    eventGeneratorFactory,
                    PARALLEL_SESSIONS,
                    NUMBER_OF_SESSIONS,
                    randomGenerator);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
