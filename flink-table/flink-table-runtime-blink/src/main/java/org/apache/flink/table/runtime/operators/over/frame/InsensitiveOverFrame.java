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

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;

/**
 * The insensitive window frame calculates the statements which shouldn't care the window frame,
 * for example RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/ROW_NUMBER.
 */
public class InsensitiveOverFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;
	private AggsHandleFunction processor;

	public InsensitiveOverFrame(
			GeneratedAggsHandleFunction aggsHandleFunction) {
		this.aggsHandleFunction = aggsHandleFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		processor = aggsHandleFunction.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		this.aggsHandleFunction = null;
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		//reset the accumulator value
		processor.setAccumulators(processor.createAccumulators());
	}

	@Override
	public BaseRow process(int index, BaseRow current) throws Exception {
		processor.accumulate(current);
		return processor.getValue();
	}
}
