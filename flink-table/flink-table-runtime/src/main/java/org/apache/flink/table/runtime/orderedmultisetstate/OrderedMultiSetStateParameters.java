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

package org.apache.flink.table.runtime.orderedmultisetstate;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;

import java.util.function.Function;

/** {@link OrderedMultiSetState} parameters. */
public class OrderedMultiSetStateParameters {

    final StateSettings stateSettings;
    final TypeSerializer<RowData> keySerializer;
    final GeneratedRecordEqualiser generatedKeyEqualiser;
    final GeneratedHashFunction generatedKeyHashFunction;
    final TypeSerializer<RowData> recordSerializer;
    final Function<RowData, RowData> keyExtractor;

    public OrderedMultiSetStateParameters(
            TypeSerializer<RowData> keySerializer,
            GeneratedRecordEqualiser generatedKeyEqualiser,
            GeneratedHashFunction generatedKeyHashFunction,
            TypeSerializer<RowData> recordSerializer,
            Function<RowData, RowData> keyExtractor,
            StateSettings stateSettings) {
        this.keySerializer = keySerializer;
        this.generatedKeyEqualiser = generatedKeyEqualiser;
        this.generatedKeyHashFunction = generatedKeyHashFunction;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
        this.stateSettings = stateSettings;
    }
}
