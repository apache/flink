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

package org.apache.flink.model.triton;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TritonInferenceModelFunction#buildDefaultResult(Object)}.
 *
 * <p>Regression guard for a subtle correctness bug: a previous implementation cached a single
 * {@code Collection<RowData>} (wrapping a single {@code GenericRowData}) and handed that same
 * instance to every failing record. Because downstream operators may retain references to the
 * emitted rows (keyed aggregations, changelog collectors, serializers that reuse field storage),
 * aliasing a single row across every fallback emission silently violates the "each emitted row is
 * independent" contract.
 */
class TritonDefaultValueFallbackTest {

    @Test
    void testEachCallReturnsFreshCollection() {
        // Aliasing the same Collection across fallbacks would let a downstream operator that
        // (legitimately) mutates its input collection (e.g. addAll / clear on the list it
        // receives) corrupt future fallback emissions. Ensure every call produces a distinct
        // container.
        Object payload = BinaryStringData.fromString("FAILED");
        Collection<RowData> first = TritonInferenceModelFunction.buildDefaultResult(payload);
        Collection<RowData> second = TritonInferenceModelFunction.buildDefaultResult(payload);

        assertThat(first).isNotSameAs(second);
    }

    @Test
    void testEachCallReturnsFreshRow() {
        // GenericRowData carries mutable RowKind plus a mutable field array; aliasing a single
        // row into every emitted record would leak RowKind changes (insert/update/delete) and
        // any per-record field overwrites back into the cached fallback, poisoning subsequent
        // emissions. Assert that the row instance itself is distinct per call.
        Object payload = BinaryStringData.fromString("FAILED");
        RowData first = firstRow(TritonInferenceModelFunction.buildDefaultResult(payload));
        RowData second = firstRow(TritonInferenceModelFunction.buildDefaultResult(payload));

        assertThat(first).isNotSameAs(second);
    }

    @Test
    void testNullPayloadProducesSqlNullField() {
        // When the user configures `default-value='null'`, parseDefaultPayload returns a Java
        // null. Wrapping that in GenericRowData must produce a single-column row whose only
        // field reads back as SQL NULL - the "no fallback configured" semantics must be carried
        // by defaultValueConfigured, not by the payload value itself.
        Collection<RowData> result = TritonInferenceModelFunction.buildDefaultResult(null);
        RowData row = firstRow(result);

        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    void testDownstreamMutationOfPriorResultDoesNotAffectNextResult() {
        // End-to-end assertion of the non-aliasing invariant: mutating the row emitted by call
        // N (here by overwriting its field and flipping RowKind) must leave call N+1 pristine.
        // Triton's fallback contract is "each record sees a clean default row".
        Object payload = BinaryStringData.fromString("FAILED");
        Collection<RowData> first = TritonInferenceModelFunction.buildDefaultResult(payload);
        GenericRowData firstRow = (GenericRowData) firstRow(first);
        firstRow.setField(0, BinaryStringData.fromString("HIJACKED"));
        firstRow.setRowKind(org.apache.flink.types.RowKind.DELETE);

        RowData second = firstRow(TritonInferenceModelFunction.buildDefaultResult(payload));
        assertThat(second.getString(0).toString()).isEqualTo("FAILED");
        assertThat(second.getRowKind()).isEqualTo(org.apache.flink.types.RowKind.INSERT);
    }

    private static RowData firstRow(Collection<RowData> collection) {
        assertThat(collection).hasSize(1);
        Iterator<RowData> it = collection.iterator();
        return it.next();
    }
}
