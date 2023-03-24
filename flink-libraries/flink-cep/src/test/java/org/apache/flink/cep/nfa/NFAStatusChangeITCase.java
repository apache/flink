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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.cep.utils.TestSharedBuffer;
import org.apache.flink.cep.utils.TestTimerService;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests if the {@link NFAState} status is changed after processing events. */
public class NFAStatusChangeITCase {

    private SharedBuffer<Event> sharedBuffer;
    private SharedBufferAccessor<Event> sharedBufferAccessor;
    private AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.noSkip();
    private TimerService timerService = new TestTimerService();

    @Before
    public void init() {
        this.sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
        sharedBufferAccessor = sharedBuffer.getAccessor();
    }

    @After
    public void clear() throws Exception {
        sharedBufferAccessor.close();
    }

    @Test
    public void testNFAChange() throws Exception {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedByAny("middle")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            8061969839441121955L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("b");
                                    }
                                })
                        .oneOrMore()
                        .optional()
                        .allowCombinations()
                        .followedBy("middle2")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            8061969839441121955L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("d");
                                    }
                                })
                        .followedBy("end")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            8061969839441121955L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("e");
                                    }
                                })
                        .within(Time.milliseconds(10));

        NFA<Event> nfa = compile(pattern, true);

        NFAState nfaState = nfa.createInitialNFAState();

        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(1, "b", 1.0),
                1L,
                skipStrategy,
                timerService);
        assertFalse(
                "NFA status should not change as the event does not match the take condition of the 'start' state",
                nfaState.isStateChanged());

        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(2, "a", 1.0),
                2L,
                skipStrategy,
                timerService);
        assertTrue(
                "NFA status should change as the event matches the take condition of the 'start' state",
                nfaState.isStateChanged());

        // the status of the queue of ComputationStatus changed,
        // more than one ComputationStatus is generated by the event from some ComputationStatus
        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(3, "f", 1.0),
                3L,
                skipStrategy,
                timerService);
        assertTrue(
                "NFA status should change as the event matches the ignore condition and proceed condition of the 'middle:1' state",
                nfaState.isStateChanged());

        // both the queue of ComputationStatus and eventSharedBuffer have not changed
        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(4, "f", 1.0),
                4L,
                skipStrategy,
                timerService);
        assertFalse(
                "NFA status should not change as the event only matches the ignore condition of the 'middle:2' state and the target state is still 'middle:2'",
                nfaState.isStateChanged());

        // both the queue of ComputationStatus and eventSharedBuffer have changed
        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(5, "b", 1.0),
                5L,
                skipStrategy,
                timerService);
        assertTrue(
                "NFA status should change as the event matches the take condition of 'middle:2' state",
                nfaState.isStateChanged());

        // both the queue of ComputationStatus and eventSharedBuffer have changed
        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(6, "d", 1.0),
                6L,
                skipStrategy,
                timerService);
        assertTrue(
                "NFA status should change as the event matches the take condition of 'middle2' state",
                nfaState.isStateChanged());

        // both the queue of ComputationStatus and eventSharedBuffer have not changed
        // as the timestamp is within the window
        nfaState.resetStateChanged();
        nfa.advanceTime(sharedBufferAccessor, nfaState, 8L, skipStrategy);
        assertFalse(
                "NFA status should not change as the timestamp is within the window",
                nfaState.isStateChanged());

        // timeout ComputationStatus will be removed from the queue of ComputationStatus and timeout
        // event will
        // be removed from eventSharedBuffer as the timeout happens
        nfaState.resetStateChanged();
        Collection<Tuple2<Map<String, List<Event>>, Long>> timeoutResults =
                nfa.advanceTime(sharedBufferAccessor, nfaState, 12L, skipStrategy).f1;
        assertTrue(
                "NFA status should change as timeout happens",
                nfaState.isStateChanged() && !timeoutResults.isEmpty());
    }

    @Test
    public void testNFAChangedOnOneNewComputationState() throws Exception {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedBy("a*")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .next("end")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            8061969839441121955L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("b");
                                    }
                                })
                        .within(Time.milliseconds(10));

        NFA<Event> nfa = compile(pattern, true);

        NFAState nfaState = nfa.createInitialNFAState();

        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(6, "start", 1.0),
                6L,
                skipStrategy,
                timerService);

        nfaState.resetStateChanged();
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(6, "a", 1.0),
                7L,
                skipStrategy,
                timerService);
        assertTrue(nfaState.isStateChanged());
    }

    @Test
    public void testNFAChangedOnTimeoutWithoutPrune() throws Exception {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("start");
                                    }
                                })
                        .followedBy("end")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            8061969839441121955L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("end");
                                    }
                                })
                        .within(Time.milliseconds(10));

        NFA<Event> nfa = compile(pattern, true);

        NFAState nfaState = nfa.createInitialNFAState();

        nfaState.resetStateChanged();
        nfa.advanceTime(sharedBufferAccessor, nfaState, 6L, skipStrategy);
        nfa.process(
                sharedBufferAccessor,
                nfaState,
                new Event(6, "start", 1.0),
                6L,
                skipStrategy,
                timerService);

        nfaState.resetStateChanged();
        nfa.advanceTime(sharedBufferAccessor, nfaState, 17L, skipStrategy);
        assertTrue(nfaState.isStateChanged());
    }
}
