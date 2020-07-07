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

package org.apache.flink.sql.tests;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end test for Stream SQL queries.
 *
 * <p>Includes the following SQL features:
 * - OVER window aggregation
 * - keyed and non-keyed GROUP BY TUMBLE aggregation
 * - windowed INNER JOIN
 * - TableSource with event-time attribute
 *
 * <p>The stream is bounded and will complete after about a minute.
 * The result is always constant.
 * The job is killed on the first attempt and restarted.
 *
 * <p>Parameters:
 * -outputPath Sets the path to where the result data is written.
 */
public class StreamSQLTestProgram {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String outputPath = params.getRequired("outputPath");
		String planner = params.get("planner", "blink");

		final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
		builder.inStreamingMode();

		if (planner.equals("old")) {
			builder.useOldPlanner();
		} else if (planner.equals("blink")) {
			builder.useBlinkPlanner();
		}

		final EnvironmentSettings settings = builder.build();

		final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			3,
			Time.of(10, TimeUnit.SECONDS)
		));
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		sEnv.enableCheckpointing(4000);
		sEnv.getConfig().setAutoWatermarkInterval(1000);

		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

		((TableEnvironmentInternal) tEnv).registerTableSourceInternal("table1", new GeneratorTableSource(10, 100, 60, 0));
		((TableEnvironmentInternal) tEnv).registerTableSourceInternal("table2", new GeneratorTableSource(5, 0.2f, 60, 5));

		int overWindowSizeSeconds = 1;
		int tumbleWindowSizeSeconds = 10;

		String overQuery = String.format(
			"SELECT " +
			"  key, " +
			"  rowtime, " +
			"  COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt " +
			"FROM table1",
			overWindowSizeSeconds);

		String tumbleQuery = String.format(
			"SELECT " +
			"  key, " +
			"  CASE SUM(cnt) / COUNT(*) WHEN 101 THEN 1 ELSE 99 END AS correct, " +
			"  TUMBLE_START(rowtime, INTERVAL '%d' SECOND) AS wStart, " +
			"  TUMBLE_ROWTIME(rowtime, INTERVAL '%d' SECOND) AS rowtime " +
			"FROM (%s) " +
			"WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01' " +
			"GROUP BY key, TUMBLE(rowtime, INTERVAL '%d' SECOND)",
			tumbleWindowSizeSeconds,
			tumbleWindowSizeSeconds,
			overQuery,
			tumbleWindowSizeSeconds);

		String joinQuery = String.format(
			"SELECT " +
			"  t1.key, " +
			"  t2.rowtime AS rowtime, " +
			"  t2.correct," +
			"  t2.wStart " +
			"FROM table2 t1, (%s) t2 " +
			"WHERE " +
			"  t1.key = t2.key AND " +
			"  t1.rowtime BETWEEN t2.rowtime AND t2.rowtime + INTERVAL '%d' SECOND",
			tumbleQuery,
			tumbleWindowSizeSeconds);

		String finalAgg = String.format(
			"SELECT " +
			"  SUM(correct) AS correct, " +
			"  TUMBLE_START(rowtime, INTERVAL '20' SECOND) AS rowtime " +
			"FROM (%s) " +
			"GROUP BY TUMBLE(rowtime, INTERVAL '20' SECOND)",
			joinQuery);

		// get Table for SQL query
		Table result = tEnv.sqlQuery(finalAgg);
		// convert Table into append-only DataStream
		DataStream<Row> resultStream =
			tEnv.toAppendStream(result, Types.ROW(Types.INT, Types.SQL_TIMESTAMP));

		final StreamingFileSink<Row> sink = StreamingFileSink
			.forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
				PrintStream out = new PrintStream(stream);
				out.println(element.toString());
			})
			.withBucketAssigner(new KeyBucketAssigner())
			.withRollingPolicy(OnCheckpointRollingPolicy.build())
			.build();

		resultStream
			// inject a KillMapper that forwards all records but terminates the first execution attempt
			.map(new KillMapper()).setParallelism(1)
			// add sink function
			.addSink(sink).setParallelism(1);

		sEnv.execute();
	}

	/**
	 * Use first field for buckets.
	 */
	public static final class KeyBucketAssigner implements BucketAssigner<Row, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(final Row element, final Context context) {
			return String.valueOf(element.getField(0));
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * TableSource for generated data.
	 */
	public static class GeneratorTableSource
		implements StreamTableSource<Row>, DefinedRowtimeAttributes, DefinedFieldMapping {

		private final int numKeys;
		private final float recordsPerKeyAndSecond;
		private final int durationSeconds;
		private final int offsetSeconds;

		public GeneratorTableSource(int numKeys, float recordsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
			this.numKeys = numKeys;
			this.recordsPerKeyAndSecond = recordsPerKeyAndSecond;
			this.durationSeconds = durationSeconds;
			this.offsetSeconds = offsetSeconds;
		}

		@Override
		public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
			return execEnv.addSource(new Generator(numKeys, recordsPerKeyAndSecond, durationSeconds, offsetSeconds));
		}

		@Override
		public TypeInformation<Row> getReturnType() {
			return Types.ROW(Types.INT, Types.LONG, Types.STRING);
		}

		@Override
		public TableSchema getTableSchema() {
			return new TableSchema(
				new String[] {"key", "rowtime", "payload"},
				new TypeInformation[] {Types.INT, Types.SQL_TIMESTAMP, Types.STRING});
		}

		@Override
		public String explainSource() {
			return "GeneratorTableSource";
		}

		@Override
		public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
			return Collections.singletonList(
				new RowtimeAttributeDescriptor(
					"rowtime",
					new ExistingField("ts"),
					new BoundedOutOfOrderTimestamps(100)));
		}

		@Override
		public Map<String, String> getFieldMapping() {
			Map<String, String> mapping = new HashMap<>();
			mapping.put("key", "f0");
			mapping.put("ts", "f1");
			mapping.put("payload", "f2");
			return mapping;
		}
	}

	/**
	 * Data-generating source function.
	 */
	public static class Generator implements SourceFunction<Row>, ResultTypeQueryable<Row>, CheckpointedFunction {

		private final int numKeys;
		private final int offsetSeconds;

		private final int sleepMs;
		private final int durationMs;

		private long ms = 0;
		private ListState<Long> state = null;

		public Generator(int numKeys, float rowsPerKeyAndSecond, int durationSeconds, int offsetSeconds) {
			this.numKeys = numKeys;
			this.durationMs = durationSeconds * 1000;
			this.offsetSeconds = offsetSeconds;

			this.sleepMs = (int) (1000 / rowsPerKeyAndSecond);
		}

		@Override
		public void run(SourceContext<Row> ctx) throws Exception {
			long offsetMS = offsetSeconds * 2000L;

			while (ms < durationMs) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Row.of(i, ms + offsetMS, "Some payload..."));
					}
					ms += sleepMs;
				}
				Thread.sleep(sleepMs);
			}
		}

		@Override
		public void cancel() { }

		@Override
		public TypeInformation<Row> getProducedType() {
			return Types.ROW(Types.INT, Types.LONG, Types.STRING);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(
					new ListStateDescriptor<Long>("state", LongSerializer.INSTANCE));

			for (Long l : state.get()) {
				ms += l;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(ms);
		}
	}

	/**
	 * Kills the first execution attempt of an application when it receives the second record.
	 */
	public static class KillMapper implements MapFunction<Row, Row>, CheckpointedFunction, ResultTypeQueryable {

		// counts all processed records of all previous execution attempts
		private int saveRecordCnt = 0;
		// counts all processed records of this execution attempt
		private int lostRecordCnt = 0;

		private ListState<Integer> state = null;

		@Override
		public Row map(Row value) {

			// the both counts are the same only in the first execution attempt
			if (saveRecordCnt == 1 && lostRecordCnt == 1) {
				throw new RuntimeException("Kill this Job!");
			}

			// update checkpointed counter
			saveRecordCnt++;
			// update non-checkpointed counter
			lostRecordCnt++;

			// forward record
			return value;
		}

		@Override
		public TypeInformation getProducedType() {
			return Types.ROW(Types.INT, Types.SQL_TIMESTAMP);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(
					new ListStateDescriptor<Integer>("state", IntSerializer.INSTANCE));

			for (Integer i : state.get()) {
				saveRecordCnt += i;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(saveRecordCnt);
		}
	}
}
