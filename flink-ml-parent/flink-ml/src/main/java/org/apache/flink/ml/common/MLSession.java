/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.common.utils.JsonConverter;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Session of Machine Learning algorithms.
 */
public class MLSession {

	public static JsonConverter jsonConverter = new JsonConverter();

	private static ExecutionEnvironment env;
	private static StreamExecutionEnvironment streamEnv;
	private static BatchTableEnvironment batchTableEnv;
	private static StreamTableEnvironment streamTableEnv;

	public static ExecutionEnvironment getExecutionEnvironment() {
		if (null == env) {
			if (ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
				Configuration conf = new Configuration();
				conf.setBoolean("taskmanager.memory.preallocate", true);
				conf.setBoolean("taskmanager.memory.off-heap", true);
				conf.setFloat("taskmanager.memory.fraction", 0.3f);
				env = ExecutionEnvironment.createLocalEnvironment(conf);
				env.setParallelism(Runtime.getRuntime().availableProcessors());
			} else {
				env = ExecutionEnvironment.getExecutionEnvironment();
			}
		}
		return env;
	}

	public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
		if (null == streamEnv) {
			streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		}
		return streamEnv;
	}

	public static BatchTableEnvironment getBatchTableEnvironment() {
		if (null == batchTableEnv) {
			batchTableEnv = BatchTableEnvironment.create(getExecutionEnvironment());
		}
		return batchTableEnv;
	}

	public static StreamTableEnvironment getStreamTableEnvironment() {
		if (null == streamTableEnv) {
			streamTableEnv = StreamTableEnvironment.create(getStreamExecutionEnvironment());
		}
		return streamTableEnv;
	}

	public static Table createBatchTable(List<Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new IllegalArgumentException("Values can not be empty.");
		}

		Row first = rows.iterator().next();
		int arity = first.getArity();

		TypeInformation<?>[] types = new TypeInformation[arity];

		for (int i = 0; i < arity; ++i) {
			types[i] = TypeExtractor.getForObject(first.getField(i));
		}

		DataSet<Row> dataSet = getExecutionEnvironment().fromCollection(rows);
		return RowTypeDataSet.toTable(dataSet, colNames, types);
	}

	public static Table createStreamTable(List<Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new IllegalArgumentException("Values can not be empty.");
		}

		Row first = rows.iterator().next();
		int arity = first.getArity();

		TypeInformation<?>[] types = new TypeInformation[arity];

		for (int i = 0; i < arity; ++i) {
			types[i] = TypeExtractor.getForObject(first.getField(i));
		}

		DataStream<Row> dataSet = getStreamExecutionEnvironment().fromCollection(rows);
		return RowTypeDataStream.toTable(dataSet, colNames, types);
	}

}

