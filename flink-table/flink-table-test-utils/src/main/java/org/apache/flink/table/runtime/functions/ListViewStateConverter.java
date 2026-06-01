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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.conversion.DataStructureConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Converter for ListView state.
 *
 * <p>Internal storage is always a {@link TtlAwareListView} with elements in converted form.
 */
@Internal
class ListViewStateConverter implements StateConverter {

    private final DataStructureConverter<Object, Object> elementConverter;
    private final LongSupplier clock;

    ListViewStateConverter(
            DataStructureConverter<Object, Object> elementConverter, LongSupplier clock) {
        this.elementConverter = elementConverter;
        this.clock = clock;
    }

    @Override
    public Object toInternal(Object external) {
        TtlAwareListView<?> view = (TtlAwareListView<?>) external;
        TtlAwareListView<Object> newView = new TtlAwareListView<>(clock);
        newView.setListWithTimestamps(
                convertElements(view.getList(), elementConverter::toInternal),
                new ArrayList<>(view.getTimestamps()));
        return newView;
    }

    @Override
    public Object toExternal(Object internal) {
        TtlAwareListView<?> view = (TtlAwareListView<?>) internal;
        TtlAwareListView<Object> newView = new TtlAwareListView<>(clock);
        newView.setListWithTimestamps(
                convertElements(view.getList(), elementConverter::toExternal),
                new ArrayList<>(view.getTimestamps()));
        return newView;
    }

    @Override
    public Object createNewInternalState() {
        return new TtlAwareListView<>(clock);
    }

    @Override
    public void evictExpired(Object internalState, long now, long ttlMillis) {
        ((TtlAwareListView<?>) internalState).evictExpired(now, ttlMillis);
    }

    private List<Object> convertElements(List<?> elements, Function<Object, Object> converter) {
        return elements.stream().map(converter).collect(Collectors.toList());
    }
}
