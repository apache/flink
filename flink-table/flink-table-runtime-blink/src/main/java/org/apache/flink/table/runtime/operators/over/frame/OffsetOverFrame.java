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

import java.io.Serializable;

/**
 * The offset window frame calculates frames containing LEAD/LAG statements.
 *
 * <p>See {@code LeadLagAggFunction}.
 */
public class OffsetOverFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;
	private final Long offset;
	private final CalcOffsetFunc calcOffsetFunc;

	private AggsHandleFunction processor;

	//inputIterator and inputIndex are need when calcOffsetFunc is null.
	private ResettableExternalBuffer.BufferIterator inputIterator;
	private long inputIndex = 0L;

	//externalBuffer is need when calcOffsetFunc is not null.
	private ResettableExternalBuffer externalBuffer;

	private long currentBufferLength = 0L;

	/**
	 * @param aggsHandleFunction the aggregate function
	 * @param offset it means the offset within a partition if calcOffsetFunc is null.
	 * @param calcOffsetFunc calculate the real offset when the function is not null.
	 */
	public OffsetOverFrame(
			GeneratedAggsHandleFunction aggsHandleFunction,
			Long offset,
			CalcOffsetFunc calcOffsetFunc) {
		this.aggsHandleFunction = aggsHandleFunction;
		this.offset = offset;
		this.calcOffsetFunc = calcOffsetFunc;
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
		currentBufferLength = rows.size();
		if (calcOffsetFunc == null) {
			inputIndex = offset;
			if (inputIterator != null) {
				inputIterator.close();
			}
			if (offset >= 0) {
				inputIterator = rows.newIterator((int) inputIndex);
			} else {
				inputIterator = rows.newIterator();
			}
		} else {
			externalBuffer = rows;
		}
	}

	@Override
	public BaseRow process(int index, BaseRow current) throws Exception {
		if (calcOffsetFunc != null) {
			//poor performance here
			long realIndex = calcOffsetFunc.calc(current) + index;
			if (realIndex >= 0 && realIndex < currentBufferLength) {
				ResettableExternalBuffer.BufferIterator tempIterator = externalBuffer.newIterator((int) realIndex);
				processor.accumulate(OverWindowFrame.getNextOrNull(tempIterator));
				tempIterator.close();
			} else {
				// reset the default based current row
				// NOTE: Special methods customized for LeadLagAggFunction.
				// TODO refactor it.
				processor.retract(current);
			}
		} else {
			if (inputIndex >= 0 && inputIndex < currentBufferLength) {
				processor.accumulate(OverWindowFrame.getNextOrNull(inputIterator));
			} else {
				//reset the default based current row
				processor.retract(current);
			}
			inputIndex += 1;
		}
		return processor.getValue();
	}

	/**
	 * Calc offset from base row.
	 */
	public interface CalcOffsetFunc extends Serializable {

		long calc(BaseRow row);
	}
}
