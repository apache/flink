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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import javax.annotation.Nonnull;

import java.util.List;

class TtlValueStateVerifier
        extends AbstractTtlStateVerifier<
                ValueStateDescriptor<String>, ValueState<String>, String, String, String> {
    TtlValueStateVerifier() {
        super(
                new ValueStateDescriptor<>(
                        TtlValueStateVerifier.class.getSimpleName(), StringSerializer.INSTANCE));
    }

    @Override
    @Nonnull
    State createState(FunctionInitializationContext context) {
        return context.getKeyedStateStore().getState(stateDesc);
    }

    @Nonnull
    public String generateRandomUpdate() {
        return randomString();
    }

    @Override
    String getInternal(@Nonnull ValueState<String> state) throws Exception {
        return state.value();
    }

    @Override
    void updateInternal(@Nonnull ValueState<String> state, String update) throws Exception {
        state.update(update);
    }

    @Override
    String expected(@Nonnull List<ValueWithTs<String>> updates, long currentTimestamp) {
        if (updates.isEmpty()) {
            return null;
        }
        ValueWithTs<String> lastUpdate = updates.get(updates.size() - 1);
        return expired(lastUpdate.getTimestamp(), currentTimestamp) ? null : lastUpdate.getValue();
    }
}
