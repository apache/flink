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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

class ReducingUpsertWriterState<WriterState> {

    private final List<WriterState> wrappedStates;
    private final Map<RowData, Tuple2<RowData, Long>> reduceBuffer;

    ReducingUpsertWriterState(
            Map<RowData, Tuple2<RowData, Long>> reduceBuffer,
            @Nullable List<WriterState> wrappedStates) {
        this.wrappedStates = wrappedStates;
        this.reduceBuffer = checkNotNull(reduceBuffer);
    }

    public List<WriterState> getWrappedStates() {
        return wrappedStates;
    }

    public Map<RowData, Tuple2<RowData, Long>> getReduceBuffer() {
        return reduceBuffer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReducingUpsertWriterState<?> that = (ReducingUpsertWriterState<?>) o;
        return Objects.equals(wrappedStates, that.wrappedStates)
                && Objects.equals(reduceBuffer, that.reduceBuffer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrappedStates, reduceBuffer);
    }
}
