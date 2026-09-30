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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.api.dataview.ValueView;

import java.io.IOException;

/**
 * {@link StateValueView} is a {@link ValueView} which is implemented using state backends.
 *
 * @param <EV> the external type of the value in the {@link ValueView}
 */
@Internal
public abstract class StateValueView<N, EV> extends ValueView<EV> implements StateDataView<N> {

    @Override
    public EV getValue() {
        try {
            return getValueState().value();
        } catch (IOException e) {
            throw new RuntimeException("Unable to get value.", e);
        }
    }

    @Override
    public void setValue(EV value) {
        try {
            if (value == null) {
                getValueState().clear();
            } else {
                getValueState().update(value);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to set value.", e);
        }
    }

    @Override
    public void clear() {
        getValueState().clear();
    }

    protected abstract ValueState<EV> getValueState();

    /**
     * {@link KeyedStateValueView} is a default implementation of {@link StateValueView} whose
     * underlying representation is a keyed state.
     */
    public static final class KeyedStateValueView<N, T> extends StateValueView<N, T> {

        private final ValueState<T> valueState;

        public KeyedStateValueView(ValueState<T> valueState) {
            this.valueState = valueState;
        }

        @Override
        public void setCurrentNamespace(N namespace) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected ValueState<T> getValueState() {
            return valueState;
        }
    }
}
