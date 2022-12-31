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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.types.RowKind;

/** The operator to set the row kind of {@link org.apache.flink.table.data.RowData}. */
@Internal
public class SinkRowDataKindEnforcer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private final RowKind rowKind;

    public SinkRowDataKindEnforcer(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        element.getValue().setRowKind(rowKind);
        output.collect(element);
    }
}
