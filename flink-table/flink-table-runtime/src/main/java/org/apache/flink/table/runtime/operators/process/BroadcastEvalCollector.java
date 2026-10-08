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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;

import java.util.function.BooleanSupplier;

/**
 * Collector for PTFs that take a table with broadcast semantics. A broadcast table can only update
 * broadcast state, thus, emitting results fails while processing a broadcast table. Otherwise, it
 * forwards to the regular collector.
 */
@Internal
public class BroadcastEvalCollector extends PassThroughCollectorBase {

    private final PassThroughCollectorBase evalCollector;
    private final BooleanSupplier isProcessingBroadcast;

    public BroadcastEvalCollector(
            Output<StreamRecord<RowData>> output,
            ChangelogMode changelogMode,
            PassThroughCollectorBase evalCollector,
            BooleanSupplier isProcessingBroadcast) {
        super(output, changelogMode, 1);
        this.evalCollector = evalCollector;
        this.isProcessingBroadcast = isProcessingBroadcast;
    }

    @Override
    public void setPrefix(int pos, RowData input) {
        evalCollector.setPrefix(pos, input);
    }

    @Override
    public void setRowtime(Long time) {
        evalCollector.setRowtime(time);
    }

    @Override
    public void collect(RowData functionOutput) {
        if (isProcessingBroadcast.getAsBoolean()) {
            throw new TableRuntimeException(
                    "Emitting results via collect() is not supported while processing a "
                            + "table with broadcast semantics.");
        }
        evalCollector.collect(functionOutput);
    }

    @Override
    public void close() {
        evalCollector.close();
    }
}
