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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

abstract class TtlMergingStateTestContext<
                S extends InternalMergingState<?, String, ?, ?, GV>, UV, GV>
        extends TtlStateTestContextBase<S, UV, GV> {
    static final Random RANDOM = new Random();

    static final List<String> NAMESPACES =
            Arrays.asList(
                    "unsetNamespace1",
                    "unsetNamespace2",
                    "expiredNamespace",
                    "expiredAndUpdatedNamespace",
                    "unexpiredNamespace",
                    "finalNamespace");

    List<Tuple2<String, UV>> generateExpiredUpdatesToMerge() {
        return Arrays.asList(
                Tuple2.of("expiredNamespace", generateRandomUpdate()),
                Tuple2.of("expiredNamespace", generateRandomUpdate()),
                Tuple2.of("expiredAndUpdatedNamespace", generateRandomUpdate()),
                Tuple2.of("expiredAndUpdatedNamespace", generateRandomUpdate()));
    }

    List<Tuple2<String, UV>> generateUnexpiredUpdatesToMerge() {
        return Arrays.asList(
                Tuple2.of("expiredAndUpdatedNamespace", generateRandomUpdate()),
                Tuple2.of("expiredAndUpdatedNamespace", generateRandomUpdate()),
                Tuple2.of("unexpiredNamespace", generateRandomUpdate()),
                Tuple2.of("unexpiredNamespace", generateRandomUpdate()));
    }

    List<Tuple2<String, UV>> generateFinalUpdatesToMerge() {
        return Arrays.asList(
                Tuple2.of("expiredAndUpdatedNamespace", generateRandomUpdate()),
                Tuple2.of("expiredAndUpdatedNamespace", generateRandomUpdate()),
                Tuple2.of("unexpiredNamespace", generateRandomUpdate()),
                Tuple2.of("unexpiredNamespace", generateRandomUpdate()),
                Tuple2.of("finalNamespace", generateRandomUpdate()),
                Tuple2.of("finalNamespace", generateRandomUpdate()));
    }

    abstract UV generateRandomUpdate();

    void applyStateUpdates(List<Tuple2<String, UV>> updates) throws Exception {
        for (Tuple2<String, UV> t : updates) {
            ttlState.setCurrentNamespace(t.f0);
            update(t.f1);
        }
    }

    abstract GV getMergeResult(
            List<Tuple2<String, UV>> unexpiredUpdatesToMerge,
            List<Tuple2<String, UV>> finalUpdatesToMerge);

    @SuppressWarnings("unchecked")
    abstract static class TtlIntegerMergingStateTestContext<
                    S extends InternalMergingState<?, String, ?, ?, GV>, UV extends Number, GV>
            extends TtlMergingStateTestContext<S, UV, GV> {
        @Override
        UV generateRandomUpdate() {
            return (UV) (Integer) RANDOM.nextInt(1000);
        }

        int getIntegerMergeResult(
                List<Tuple2<String, UV>> unexpiredUpdatesToMerge,
                List<Tuple2<String, UV>> finalUpdatesToMerge) {
            return unexpiredUpdatesToMerge.stream().mapToInt(t -> (Integer) t.f1).sum()
                    + finalUpdatesToMerge.stream().mapToInt(t -> (Integer) t.f1).sum();
        }
    }
}
