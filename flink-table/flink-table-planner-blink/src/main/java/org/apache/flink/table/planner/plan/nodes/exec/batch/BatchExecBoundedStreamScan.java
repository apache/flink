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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.BatchPlanner;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Batch exec node to connect a given bounded {@link DataStream} and consume data from it.
 */
public class BatchExecBoundedStreamScan extends BatchExecNode<RowData> {
	private final DataStream<?> dataStream;
	private final DataType sourceType;
	private final int[] fieldIndexes;
	private final List<String> qualifiedName;

	public BatchExecBoundedStreamScan(
			DataStream<?> dataStream,
			DataType sourceType,
			int[] fieldIndexes,
			List<String> qualifiedName,
			RowType outputType,
			String description) {
		super(Collections.emptyList(), outputType, description);
		this.dataStream = dataStream;
		this.sourceType = sourceType;
		this.fieldIndexes = fieldIndexes;
		this.qualifiedName = qualifiedName;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Transformation<RowData> translateToPlanInternal(BatchPlanner planner) {
		final Transformation<?> sourceTransform = dataStream.getTransformation();
		if (needInternalConversion()) {
			return ScanUtil.convertToInternalRow(
					new CodeGeneratorContext(planner.getTableConfig()),
					(Transformation<Object>) sourceTransform,
					fieldIndexes,
					sourceType,
					(RowType) getOutputType(),
					qualifiedName,
					JavaScalaConversionUtil.toScala(Optional.empty()),
					"",
					"");
		} else {
			return (Transformation<RowData>) sourceTransform;
		}
	}

	private boolean needInternalConversion() {
		return ScanUtil.hasTimeAttributeField(fieldIndexes) || ScanUtil.needsConversion(sourceType);
	}

	public DataStream<?> getDataStream() {
		return dataStream;
	}
}
