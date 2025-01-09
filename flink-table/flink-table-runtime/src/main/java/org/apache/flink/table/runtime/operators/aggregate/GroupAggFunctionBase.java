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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.async.AsyncStateGroupAggFunction;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/** Base class for {@link GroupAggFunction} and {@link AsyncStateGroupAggFunction}. */
public abstract class GroupAggFunctionBase extends KeyedProcessFunction<RowData, RowData, RowData> {

    /** The code generated function used to handle aggregates. */
    protected final GeneratedAggsHandleFunction genAggsHandler;

    /** The code generated equaliser used to equal RowData. */
    protected final GeneratedRecordEqualiser genRecordEqualiser;

    /** The accumulator types. */
    protected final LogicalType[] accTypes;

    /** Used to count the number of added and retracted input records. */
    protected final RecordCounter recordCounter;

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    protected final boolean generateUpdateBefore;

    /** State idle retention config. */
    protected final StateTtlConfig ttlConfig;

    // function used to handle all aggregates
    protected transient AggsHandleFunction function = null;

    // function used to equal RowData
    protected transient RecordEqualiser equaliser = null;

    public GroupAggFunctionBase(
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            LogicalType[] accTypes,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long stateRetentionTime) {
        this.genAggsHandler = genAggsHandler;
        this.genRecordEqualiser = genRecordEqualiser;
        this.accTypes = accTypes;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.ttlConfig = createTtlConfig(stateRetentionTime);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        // instantiate function
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext(), ttlConfig));
        // instantiate equaliser
        equaliser = genRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (function != null) {
            function.close();
        }
    }
}
