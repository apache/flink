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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Internal
class XaSinkStateHandlerImpl implements XaSinkStateHandler {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(XaSinkStateHandlerImpl.class);

    private final TypeSerializer<JdbcXaSinkFunctionState> serializer;

    // state could be stored as two separate lists
    // on one hand this would allow more even distribution on re-scale
    // on the other it would lead to more IO calls and less data locality
    private transient ListState<JdbcXaSinkFunctionState> states;

    XaSinkStateHandlerImpl() {
        this(new XaSinkStateSerializer());
    }

    XaSinkStateHandlerImpl(TypeSerializer<JdbcXaSinkFunctionState> serializer) {
        this.serializer = serializer;
    }

    @Override
    public JdbcXaSinkFunctionState load(FunctionInitializationContext context) throws Exception {
        states = getListState(context, serializer, "XaSinkState");
        return context.isRestored() ? merge(states.get()) : JdbcXaSinkFunctionState.empty();
    }

    @Override
    public void store(JdbcXaSinkFunctionState state) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("store state snapshot: {}", state);
        }
        states.update(Collections.singletonList(state));
    }

    private <T> ListState<T> getListState(
            FunctionInitializationContext context, TypeSerializer<T> serializer, String name) {
        try {
            return context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>(name, serializer));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JdbcXaSinkFunctionState merge(@Nullable Iterable<JdbcXaSinkFunctionState> states) {
        if (states == null) {
            return JdbcXaSinkFunctionState.empty();
        }
        List<Xid> hanging = new ArrayList<>();
        List<CheckpointAndXid> prepared = new ArrayList<>();
        states.forEach(
                i -> {
                    hanging.addAll(i.getHanging());
                    prepared.addAll(i.getPrepared());
                });
        return JdbcXaSinkFunctionState.of(prepared, hanging);
    }
}
