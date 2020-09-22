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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.arrow.serializers.RowDataArrowSerializer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.runtime.operators.python.utils.PythonOperatorUtils.getUserDefinedFunctionProto;

/**
 * The Abstract class of Arrow Aggregate Operator for Pandas {@link AggregateFunction}.
 */
@Internal
public abstract class AbstractArrowPythonAggregateFunctionOperator
	extends AbstractStatelessFunctionOperator<RowData, RowData, RowData> {

	private static final long serialVersionUID = 1L;

	private static final String SCHEMA_ARROW_CODER_URN = "flink:coder:schema:arrow:v1";

	private static final String PANDAS_AGGREGATE_FUNCTION_URN = "flink:transform:aggregate_function:arrow:v1";

	/**
	 * The Pandas {@link AggregateFunction}s to be executed.
	 */
	protected final PythonFunctionInfo[] pandasAggFunctions;

	protected final int[] groupingSet;

	protected transient ArrowSerializer<RowData> arrowSerializer;

	/**
	 * The collector used to collect records.
	 */
	protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

	/**
	 * The JoinedRowData reused holding the execution result.
	 */
	protected transient JoinedRowData reuseJoinedRow;

	/**
	 * The current number of elements to be included in an arrow batch.
	 */
	protected transient int currentBatchCount;

	/**
	 * The Projection which projects the udaf input fields from the input row.
	 */
	private transient Projection<RowData, BinaryRowData> udafInputProjection;

	public AbstractArrowPythonAggregateFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] pandasAggFunctions,
		RowType inputType,
		RowType outputType,
		int[] groupingSet,
		int[] udafInputOffsets) {
		super(config, inputType, outputType, udafInputOffsets);
		this.pandasAggFunctions = Preconditions.checkNotNull(pandasAggFunctions);
		this.groupingSet = Preconditions.checkNotNull(groupingSet);
	}

	@Override
	public void open() throws Exception {
		super.open();
		rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
		reuseJoinedRow = new JoinedRowData();

		udafInputProjection = createUdafInputProjection();
		arrowSerializer = new RowDataArrowSerializer(userDefinedFunctionInputType, userDefinedFunctionOutputType);
		arrowSerializer.open(bais, baos);
		currentBatchCount = 0;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		arrowSerializer.close();
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
	public String getInputOutputCoderUrn() {
		return SCHEMA_ARROW_CODER_URN;
	}

	@Override
	public RowData getFunctionInput(RowData element) {
		return udafInputProjection.apply(element);
	}

	@Override
	public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		// add udaf proto
		for (PythonFunctionInfo pythonFunctionInfo : pandasAggFunctions) {
			builder.addUdfs(getUserDefinedFunctionProto(pythonFunctionInfo));
		}
		builder.setMetricEnabled(getPythonConfig().isMetricEnabled());
		return builder.build();
	}

	private Projection<RowData, BinaryRowData> createUdafInputProjection() {
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"UadfInputProjection",
			inputType,
			userDefinedFunctionInputType,
			userDefinedFunctionInputOffsets);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}
}
