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

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.core.JoinRelType;


/**
 * The Python {@link TableFunction} operator for the legacy planner.
 */
@Internal
public class PythonTableFunctionOperator extends AbstractPythonTableFunctionOperator<CRow, CRow, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordCRowWrappingCollector cRowWrapper;

	/**
	 * The type serializer for the forwarded fields.
	 */
	private transient TypeSerializer<CRow> forwardedInputSerializer;

	/**
	 * The TypeSerializer for udtf execution results.
	 */
	private transient TypeSerializer<Row> udtfOutputTypeSerializer;

	/**
	 * The TypeSerializer for udtf input elements.
	 */
	private transient TypeSerializer<Row> udtfInputTypeSerializer;

	public PythonTableFunctionOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets,
		JoinRelType joinType) {
		super(config, tableFunction, inputType, outputType, udtfInputOffsets, joinType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		this.cRowWrapper = new StreamRecordCRowWrappingCollector(output);
		CRowTypeInfo forwardedInputTypeInfo = new CRowTypeInfo(
			(RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(
				TypeConversions.fromLogicalToDataType(inputType)));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getExecutionConfig());
		udtfOutputTypeSerializer = PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionOutputType);
		udtfInputTypeSerializer = PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionInputType);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		CRow input = forwardedInputQueue.poll();
		byte[] rawUdtfResult;
		int length;
		boolean isFinishResult;
		boolean hasJoined = false;
		Row udtfResult;
		do {
			rawUdtfResult = resultTuple.f0;
			length = resultTuple.f1;
			isFinishResult = isFinishResult(rawUdtfResult, length);
			if (!isFinishResult) {
				bais.setBuffer(rawUdtfResult, 0, length);
				udtfResult = udtfOutputTypeSerializer.deserialize(baisWrapper);
				cRowWrapper.setChange(input.change());
				cRowWrapper.collect(Row.join(input.row(), udtfResult));
				resultTuple = pythonFunctionRunner.pollResult();
				hasJoined = true;
			} else if (joinType == JoinRelType.LEFT && !hasJoined) {
				udtfResult = new Row(userDefinedFunctionOutputType.getFieldCount());
				for (int i = 0; i < udtfResult.getArity(); i++) {
					udtfResult.setField(0, null);
				}
				cRowWrapper.setChange(input.change());
				cRowWrapper.collect(Row.join(input.row(), udtfResult));
			}
		} while (!isFinishResult);
	}

	@Override
	public void bufferInput(CRow input) {
		if (getExecutionConfig().isObjectReuseEnabled()) {
			input = forwardedInputSerializer.copy(input);
		}
		forwardedInputQueue.add(input);
	}

	@Override
	public Row getFunctionInput(CRow element) {
		return Row.project(element.row(), userDefinedFunctionInputOffsets);
	}

	@Override
	public void processElementInternal(CRow value) throws Exception {
		udtfInputTypeSerializer.serialize(getFunctionInput(value), baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
	}
}
