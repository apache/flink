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

package org.apache.flink.streaming.tests.verify;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

class TtlMapStateVerifier
        extends AbstractTtlStateVerifier<
                MapStateDescriptor<String, String>,
                MapState<String, String>,
                Map<String, String>,
                Tuple2<String, String>,
                Map<String, String>> {
    private static final List<String> KEYS = new ArrayList<>();

    static {
        IntStream.range(0, RANDOM.nextInt(5) + 5).forEach(i -> KEYS.add(randomString()));
    }

    TtlMapStateVerifier() {
        super(
                new MapStateDescriptor<>(
                        TtlMapStateVerifier.class.getSimpleName(),
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE));
    }

    @Override
    @Nonnull
    State createState(@Nonnull FunctionInitializationContext context) {
        return context.getKeyedStateStore().getMapState(stateDesc);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nonnull
    public TypeSerializer<Tuple2<String, String>> getUpdateSerializer() {
        return new TupleSerializer(
                Tuple2.class,
                new TypeSerializer[] {StringSerializer.INSTANCE, StringSerializer.INSTANCE});
    }

    @Override
    @Nonnull
    public Tuple2<String, String> generateRandomUpdate() {
        return Tuple2.of(KEYS.get(RANDOM.nextInt(KEYS.size())), randomString());
    }

    @Override
    @Nonnull
    Map<String, String> getInternal(@Nonnull MapState<String, String> state) throws Exception {
        return StreamSupport.stream(state.entries().spliterator(), false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    void updateInternal(@Nonnull MapState<String, String> state, Tuple2<String, String> update)
            throws Exception {
        state.put(update.f0, update.f1);
    }

    @Override
    @Nonnull
    Map<String, String> expected(
            @Nonnull List<ValueWithTs<Tuple2<String, String>>> updates, long currentTimestamp) {
        return updates.stream()
                .collect(Collectors.groupingBy(u -> u.getValue().f0))
                .entrySet()
                .stream()
                .map(e -> e.getValue().get(e.getValue().size() - 1))
                .filter(u -> !expired(u.getTimestamp(), currentTimestamp))
                .map(ValueWithTs::getValue)
                .collect(Collectors.toMap(u -> u.f0, u -> u.f1));
    }
}
