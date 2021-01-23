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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * This mapper validates sliding event time window. It checks each event belongs to appropriate
 * number of consecutive windows.
 */
public class SlidingWindowCheckMapper
        extends RichFlatMapFunction<Tuple2<Integer, List<Event>>, String> {

    private static final long serialVersionUID = -744070793650644485L;

    /**
     * This value state tracks previously seen events with the number of windows they appeared in.
     */
    private transient ValueState<List<Tuple2<Event, Integer>>> eventsSeenSoFar;

    private transient ValueState<Long> lastSequenceNumber;

    private final int slideFactor;

    SlidingWindowCheckMapper(int slideFactor) {
        this.slideFactor = slideFactor;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<List<Tuple2<Event, Integer>>> previousWindowDescriptor =
                new ValueStateDescriptor<>(
                        "eventsSeenSoFar",
                        new ListTypeInfo<>(
                                new TupleTypeInfo<>(
                                        TypeInformation.of(Event.class),
                                        BasicTypeInfo.INT_TYPE_INFO)));

        eventsSeenSoFar = getRuntimeContext().getState(previousWindowDescriptor);

        ValueStateDescriptor<Long> lastSequenceNumberDescriptor =
                new ValueStateDescriptor<>("lastSequenceNumber", BasicTypeInfo.LONG_TYPE_INFO);

        lastSequenceNumber = getRuntimeContext().getState(lastSequenceNumberDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Integer, List<Event>> value, Collector<String> out)
            throws Exception {
        List<Tuple2<Event, Integer>> previousWindowValues =
                Optional.ofNullable(eventsSeenSoFar.value()).orElseGet(Collections::emptyList);

        List<Event> newValues = value.f1;
        Optional<Event> lastEventInWindow = verifyWindowContiguity(newValues, out);

        Long lastSequenceNumberSeenSoFar = lastSequenceNumber.value();
        List<Tuple2<Event, Integer>> newWindows =
                verifyPreviousOccurences(
                        previousWindowValues, newValues, lastSequenceNumberSeenSoFar, out);

        if (lastEventInWindow.isPresent()) {
            updateLastSeenSequenceNumber(lastEventInWindow.get(), lastSequenceNumberSeenSoFar, out);
        }

        eventsSeenSoFar.update(newWindows);
    }

    private void updateLastSeenSequenceNumber(
            Event lastEventInWindow, Long lastSequenceNumberSeenSoFar, Collector<String> out)
            throws IOException {
        long lastSequenceNumberInWindow = lastEventInWindow.getSequenceNumber();
        if (lastSequenceNumberSeenSoFar == null
                || lastSequenceNumberInWindow > lastSequenceNumberSeenSoFar) {
            lastSequenceNumber.update(lastSequenceNumberInWindow);
        } else if (lastSequenceNumberInWindow < lastSequenceNumberSeenSoFar) {
            failWithSequenceNumberDecreased(lastEventInWindow, lastSequenceNumberSeenSoFar, out);
        }
    }

    private void failWithSequenceNumberDecreased(
            Event lastEventInWindow, Long lastSequenceNumberSeenSoFar, Collector<String> out) {
        out.collect(
                String.format(
                        "Last event in current window (%s) has sequence number lower than seen so far (%d)",
                        lastEventInWindow, lastSequenceNumberSeenSoFar));
    }

    /**
     * Verifies if all values from previous windows appear in the new one. Returns union of all
     * events seen so far that were not seen <b>slideFactor</b> number of times yet.
     */
    private List<Tuple2<Event, Integer>> verifyPreviousOccurences(
            List<Tuple2<Event, Integer>> previousWindowValues,
            List<Event> newValues,
            Long lastSequenceNumberSeenSoFar,
            Collector<String> out) {
        List<Tuple2<Event, Integer>> newEventsSeenSoFar = new ArrayList<>();
        List<Event> seenEvents = new ArrayList<>();

        for (Tuple2<Event, Integer> windowValue : previousWindowValues) {
            if (!newValues.contains(windowValue.f0)) {
                failWithEventNotSeenAlertMessage(windowValue, newValues, out);
            } else {
                seenEvents.add(windowValue.f0);
                preserveOrDiscardIfSeenSlideFactorTimes(newEventsSeenSoFar, windowValue);
            }
        }

        addNotSeenValues(
                newEventsSeenSoFar, newValues, seenEvents, lastSequenceNumberSeenSoFar, out);

        return newEventsSeenSoFar;
    }

    private void addNotSeenValues(
            List<Tuple2<Event, Integer>> newEventsSeenSoFar,
            List<Event> newValues,
            List<Event> seenValues,
            Long lastSequenceNumberSeenSoFar,
            Collector<String> out) {
        newValues.stream()
                .filter(e -> !seenValues.contains(e))
                .forEach(
                        e -> {
                            if (lastSequenceNumberSeenSoFar == null
                                    || e.getSequenceNumber() > lastSequenceNumberSeenSoFar) {
                                newEventsSeenSoFar.add(Tuple2.of(e, 1));
                            } else {
                                failWithEventSeenTooManyTimesMessage(e, out);
                            }
                        });
    }

    private void failWithEventSeenTooManyTimesMessage(Event e, Collector<String> out) {
        out.collect(String.format("Alert: event %s seen more than %d times", e, slideFactor));
    }

    private void preserveOrDiscardIfSeenSlideFactorTimes(
            List<Tuple2<Event, Integer>> newEvenstSeenSoFar, Tuple2<Event, Integer> windowValue) {
        int timesSeen = windowValue.f1 + 1;
        if (timesSeen != slideFactor) {
            newEvenstSeenSoFar.add(Tuple2.of(windowValue.f0, timesSeen));
        }
    }

    private void failWithEventNotSeenAlertMessage(
            Tuple2<Event, Integer> previousWindowValue,
            List<Event> currentWindowValues,
            Collector<String> out) {
        out.collect(
                String.format(
                        "Alert: event %s did not belong to %d consecutive windows. "
                                + "Event seen so far %d times.Current window: %s",
                        previousWindowValue.f0,
                        slideFactor,
                        previousWindowValue.f1,
                        currentWindowValues));
    }

    private Optional<Event> verifyWindowContiguity(List<Event> newValues, Collector<String> out) {
        return newValues.stream()
                .sorted(Comparator.comparingLong(Event::getSequenceNumber))
                .reduce(
                        (event, event2) -> {
                            if (event2.getSequenceNumber() - 1 != event.getSequenceNumber()) {
                                out.collect("Alert: events in window out ouf order!");
                            }

                            return event2;
                        });
    }
}
