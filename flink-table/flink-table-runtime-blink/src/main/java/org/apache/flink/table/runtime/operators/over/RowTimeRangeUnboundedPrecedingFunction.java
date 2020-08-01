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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * A ProcessFunction to support unbounded RANGE window.
 * The RANGE option includes all the rows within the window frame
 * that have the same ORDER BY values as the current row.
 *
 * <p>E.g.:
 * SELECT rowtime, b, c,
 * min(c) OVER
 * (PARTITION BY b ORDER BY rowtime
 * RANGE BETWEEN UNBOUNDED preceding AND CURRENT ROW),
 * max(c) OVER
 * (PARTITION BY b ORDER BY rowtime
 * RANGE BETWEEN UNBOUNDED preceding AND CURRENT ROW)
 * FROM T.
 */
public class RowTimeRangeUnboundedPrecedingFunction<K> extends AbstractRowTimeUnboundedPrecedingOver<K> {
	private static final long serialVersionUID = 1L;

	public RowTimeRangeUnboundedPrecedingFunction(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			LogicalType[] accTypes,
			LogicalType[] inputFieldTypes,
			int rowTimeIdx) {
		super(minRetentionTime, maxRetentionTime, genAggsHandler, accTypes, inputFieldTypes, rowTimeIdx);
	}

	@Override
	public void processElementsWithSameTimestamp(
			List<RowData> curRowList,
			Collector<RowData> out) throws Exception {
		int i = 0;
		// all same timestamp data should have same aggregation value.
		while (i < curRowList.size()) {
			RowData curRow = curRowList.get(i);
			function.accumulate(curRow);
			i += 1;
		}

		// emit output row
		i = 0;
		RowData aggValue = function.getValue();
		while (i < curRowList.size()) {
			RowData curRow = curRowList.get(i);
			// prepare output row
			output.replace(curRow, aggValue);
			out.collect(output);
			i += 1;
		}
	}
}
