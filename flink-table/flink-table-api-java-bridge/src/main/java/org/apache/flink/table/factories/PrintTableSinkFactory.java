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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Print table sink factory writing every row to the standard output or standard error stream.
 * It is designed for:
 * - easy test for streaming job.
 * - very useful in production debugging.
 *
 * <p>
 * Four possible format options:
 *	{@code PRINT_IDENTIFIER}:taskId> output  <- {@code PRINT_IDENTIFIER} provided, parallelism > 1
 *	{@code PRINT_IDENTIFIER}> output         <- {@code PRINT_IDENTIFIER} provided, parallelism == 1
 *  taskId> output         				   <- no {@code PRINT_IDENTIFIER} provided, parallelism > 1
 *  output                 				   <- no {@code PRINT_IDENTIFIER} provided, parallelism == 1
 * </p>
 *
 * <p>output string format is "$RowKind(f0,f1,f2...)", example is: "+I(1,1)".
 */
@PublicEvolving
public class PrintTableSinkFactory implements DynamicTableSinkFactory {

	public static final String IDENTIFIER = "print";

	public static final ConfigOption<String> PRINT_IDENTIFIER = key("print-identifier")
			.stringType()
			.noDefaultValue()
			.withDescription("Message that identify print and is prefixed to the output of the value.");

	public static final ConfigOption<Boolean> STANDARD_ERROR = key("standard-error")
			.booleanType()
			.defaultValue(false)
			.withDescription("True, if the format should print to standard error instead of standard out.");

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return new HashSet<>();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PRINT_IDENTIFIER);
		options.add(STANDARD_ERROR);
		options.add(FactoryUtil.SINK_PARALLELISM);
		return options;
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig options = helper.getOptions();
		return new PrintSink(
				context.getCatalogTable().getSchema().toPhysicalRowDataType(),
				options.get(PRINT_IDENTIFIER),
				options.get(STANDARD_ERROR),
				options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
	}

	private static class PrintSink implements DynamicTableSink {

		private final DataType type;
		private final String printIdentifier;
		private final boolean stdErr;
		private final @Nullable Integer parallelism;

		private PrintSink(DataType type, String printIdentifier, boolean stdErr, Integer parallelism) {
			this.type = type;
			this.printIdentifier = printIdentifier;
			this.stdErr = stdErr;
			this.parallelism = parallelism;
		}

		@Override
		public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
			return requestedMode;
		}

		@Override
		public SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
			DataStructureConverter converter = context.createDataStructureConverter(type);
			return SinkFunctionProvider.of(new RowDataPrintFunction(converter, printIdentifier, stdErr), parallelism);
		}

		@Override
		public DynamicTableSink copy() {
			return new PrintSink(type, printIdentifier, stdErr, parallelism);
		}

		@Override
		public String asSummaryString() {
			return "Print to " + (stdErr ? "System.err" : "System.out");
		}
	}

	/**
	 * Implementation of the SinkFunction converting {@link RowData} to string and
	 * passing to {@link PrintSinkFunction}.
	 */
	private static class RowDataPrintFunction extends RichSinkFunction<RowData> {

		private static final long serialVersionUID = 1L;

		private final DataStructureConverter converter;
		private final PrintSinkOutputWriter<String> writer;

		private RowDataPrintFunction(
				DataStructureConverter converter, String printIdentifier, boolean stdErr) {
			this.converter = converter;
			this.writer = new PrintSinkOutputWriter<>(printIdentifier, stdErr);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
			writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
		}

		@Override
		public void invoke(RowData value, Context context) {
			String rowKind = value.getRowKind().shortString();
			Object data = converter.toExternal(value);
			writer.write(rowKind + "(" + data + ")");
		}
	}
}
