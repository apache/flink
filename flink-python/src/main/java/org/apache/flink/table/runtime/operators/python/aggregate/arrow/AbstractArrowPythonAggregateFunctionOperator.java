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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createArrowTypeCoderInfoDescriptorProto;

/** The Abstract class of Arrow Aggregate Operator for Pandas {@link AggregateFunction}. */
@Internal
public abstract class AbstractArrowPythonAggregateFunctionOperator
        extends AbstractStatelessFunctionOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final String PANDAS_AGGREGATE_FUNCTION_URN =
            "flink:transform:aggregate_function:arrow:v1";

    /** The Pandas {@link AggregateFunction}s to be executed. */
    protected final PythonFunctionInfo[] pandasAggFunctions;

    private final GeneratedProjection udafInputGeneratedProjection;

    protected transient ArrowSerializer arrowSerializer;

    /** The collector used to collect records. */
    protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** The JoinedRowData reused holding the execution result. */
    protected transient JoinedRowData reuseJoinedRow;

    /** The current number of elements to be included in an arrow batch. */
    protected transient int currentBatchCount;

    /** The Projection which projects the udaf input fields from the input row. */
    private transient Projection<RowData, BinaryRowData> udafInputProjection;

    public AbstractArrowPythonAggregateFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection udafInputGeneratedProjection) {
        super(config, inputType, udfInputType, udfOutputType);
        this.pandasAggFunctions = Preconditions.checkNotNull(pandasAggFunctions);
        this.udafInputGeneratedProjection =
                Preconditions.checkNotNull(udafInputGeneratedProjection);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open() throws Exception {
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseJoinedRow = new JoinedRowData();

        udafInputProjection =
                udafInputGeneratedProjection.newInstance(
                        Thread.currentThread().getContextClassLoader());
        arrowSerializer = new ArrowSerializer(udfInputType, udfOutputType);
        arrowSerializer.open(bais, baos);
        currentBatchCount = 0;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (arrowSerializer != null) {
            arrowSerializer.close();
            arrowSerializer = null;
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData value = element.getValue();
        bufferInput(value);
        processElementInternal(value);
        emitResults();
    }

    @Override
    public boolean isBundleFinished() {
        return elementCount == 0 && currentBatchCount == 0;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pandasAggFunctions[0].getPythonFunction().getPythonEnv();
    }

    @Override
    public String getFunctionUrn() {
        return PANDAS_AGGREGATE_FUNCTION_URN;
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(RowType runnerInputType) {
        return createArrowTypeCoderInfoDescriptorProto(
                runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(RowType runnerOutType) {
        return createArrowTypeCoderInfoDescriptorProto(
                runnerOutType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false);
    }

    @Override
    public RowData getFunctionInput(RowData element) {
        return udafInputProjection.apply(element);
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        return ProtoUtils.createUserDefinedFunctionsProto(
                getRuntimeContext(),
                pandasAggFunctions,
                config.get(PYTHON_METRIC_ENABLED),
                config.get(PYTHON_PROFILE_ENABLED));
    }
}
