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

package org.apache.flink.table.planner.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link TableSourceQueryOperation} with {@link FlinkStatistic} and qualifiedName.
 * TODO this class should be deleted after unique key in TableSchema is ready
 * and setting catalog statistic to TableSourceTable in DatabaseCalciteSchema is ready
 *
 * <p>This is only used for testing.
 */
@Internal
public class RichTableSourceQueryOperation<T> extends TableSourceQueryOperation<T> {
	private final FlinkStatistic statistic;
	private List<String> qualifiedName;

	public RichTableSourceQueryOperation(
			TableSource<T> tableSource,
			FlinkStatistic statistic) {
		super(tableSource, false);
		Preconditions.checkArgument(tableSource instanceof StreamTableSource,
				"Blink planner should always use StreamTableSource.");
		this.statistic = statistic;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new HashMap<>();
		args.put("fields", getTableSource().getTableSchema().getFieldNames());
		if (statistic != FlinkStatistic.UNKNOWN()) {
			args.put("statistic", statistic.toString());
		}

		return OperationUtils.formatWithChildren("TableSource", args, getChildren(), Operation::asSummaryString);
	}

	public List<String> getQualifiedName() {
		return qualifiedName;
	}

	public void setQualifiedName(List<String> qualifiedName) {
		this.qualifiedName = qualifiedName;
	}

	public FlinkStatistic getStatistic() {
		return statistic;
	}
}
