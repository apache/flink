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

package org.apache.flink.table.runtime.sequencedmultisetstate;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;

import java.io.Serializable;
import java.util.function.Function;

/** {@link SequencedMultiSetState} (creation) context. */
public class SequencedMultiSetStateContext implements Serializable {

    private static final long serialVersionUID = 1L;

    public final SequencedMultiSetStateConfig config;
    public final TypeSerializer<RowData> keySerializer;
    public final GeneratedRecordEqualiser generatedKeyEqualiser;
    public final GeneratedHashFunction generatedKeyHashFunction;
    public final TypeSerializer<RowData> recordSerializer;
    public final KeyExtractor keyExtractor;

    /** */
    public interface KeyExtractor extends Function<RowData, RowData>, Serializable {}

    public SequencedMultiSetStateContext(
            TypeSerializer<RowData> keySerializer,
            GeneratedRecordEqualiser generatedKeyEqualiser,
            GeneratedHashFunction generatedKeyHashFunction,
            TypeSerializer<RowData> recordSerializer,
            KeyExtractor keyExtractor,
            SequencedMultiSetStateConfig config) {
        this.keySerializer = keySerializer;
        this.generatedKeyEqualiser = generatedKeyEqualiser;
        this.generatedKeyHashFunction = generatedKeyHashFunction;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
        this.config = config;
    }
}
