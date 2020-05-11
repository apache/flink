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

package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 */
public class UnboundedOverWindowFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;
	private final RowType valueType;

	private AggsHandleFunction processor;
	private RowData accValue;

	private RowDataSerializer valueSer;

	public UnboundedOverWindowFrame(
			GeneratedAggsHandleFunction aggsHandleFunction,
			RowType valueType) {
		this.aggsHandleFunction = aggsHandleFunction;
		this.valueType = valueType;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		ClassLoader cl = ctx.getRuntimeContext().getUserCodeClassLoader();
		processor = aggsHandleFunction.newInstance(cl);
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));
		this.aggsHandleFunction = null;
		this.valueSer = new RowDataSerializer(
				ctx.getRuntimeContext().getExecutionConfig(),
				valueType.getChildren().toArray(new LogicalType[0]));
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		//cleanup the retired accumulators value
		processor.setAccumulators(processor.createAccumulators());
		ResettableExternalBuffer.BufferIterator iterator = rows.newIterator();
		while (iterator.advanceNext()) {
			processor.accumulate(iterator.getRow());
		}
		accValue = valueSer.copy(processor.getValue());
		iterator.close();
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		return accValue;
	}
}
