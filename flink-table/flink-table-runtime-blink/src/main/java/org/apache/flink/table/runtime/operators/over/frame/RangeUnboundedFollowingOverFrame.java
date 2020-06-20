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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.RowType;

/**
 * The range unboundedFollowing window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN [window frame preceding] AND UNBOUNDED FOLLOWING
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING.
 */
public class RangeUnboundedFollowingOverFrame extends UnboundedFollowingOverFrame {

	private GeneratedRecordComparator boundComparator;
	private RecordComparator lbound;

	public RangeUnboundedFollowingOverFrame(
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction,
			GeneratedRecordComparator boundComparator) {
		super(valueType, aggsHandleFunction);
		this.boundComparator = boundComparator;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		lbound = boundComparator.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		this.boundComparator = null;
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		boolean bufferUpdated = index == 0;

		// Ignore all the rows from the buffer for which the input row value is smaller than
		// the output row lower bound.
		ResettableExternalBuffer.BufferIterator iterator = input.newIterator(inputIndex);

		BinaryRowData nextRow = OverWindowFrame.getNextOrNull(iterator);
		while (nextRow != null && lbound.compare(nextRow, current) < 0) {
			inputIndex += 1;
			bufferUpdated = true;
			nextRow = OverWindowFrame.getNextOrNull(iterator);
		}

		return accumulateIterator(bufferUpdated, nextRow, iterator);
	}
}
