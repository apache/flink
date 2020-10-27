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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveTableInputFormat;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A utility class to calculate parallelism for Hive connector considering various factors.
 */
public class HiveParallelismInference {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParallelismInference.class);

	private final ObjectPath tablePath;
	private final ReadableConfig flinkConf;

	private long limit;
	private HiveTableInputFormat inputFormat;

	public HiveParallelismInference(ObjectPath tablePath, ReadableConfig flinkConf) {
		this.tablePath = tablePath;
		this.flinkConf = flinkConf;

		this.limit = -1L;
		this.inputFormat = null;
	}

	public HiveParallelismInference limit(long limit) {
		this.limit = limit;
		return this;
	}

	public HiveParallelismInference inputFormat(HiveTableInputFormat inputFormat) {
		this.inputFormat = inputFormat;
		return this;
	}

	public int infer() {
		int parallelism;
		if (flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM)) {
			parallelism = inferFromInputSplit();
		} else {
			parallelism = flinkConf.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
		}

		// apply limit
		if (limit > 0) {
			parallelism = Math.min(parallelism, (int) limit / 1000);
		}

		// make sure that parallelism is at least 1
		return Math.max(1, parallelism);
	}

	private int inferFromInputSplit() {
		Preconditions.checkNotNull(
			inputFormat,
			"Input format must be provided when " +
				HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM.key() + " is set");

		int max = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX);
		Preconditions.checkArgument(
			max >= 1,
			HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX.key() + " cannot be less than 1");

		try {
			// `createInputSplits` is costly,
			// so we try to avoid calling it by first checking the number of files
			// which is the lower bound of the number of splits
			int lowerBound = logRunningTime("getNumFiles", inputFormat::getNumFiles);
			if (lowerBound >= max) {
				return max;
			}

			int splitNum = logRunningTime("createInputSplits", () -> inputFormat.createInputSplits(0).length);
			return Math.min(splitNum, max);
		} catch (IOException e) {
			throw new FlinkHiveException(e);
		}
	}

	private int logRunningTime(String operationName, ThrowingIntSupplier supplier) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		int result = supplier.getAsInt();
		LOG.info(
			"Hive source({}}) {} use time: {} ms, result: {}",
			tablePath,
			operationName,
			System.currentTimeMillis() - startTimeMillis,
			result);
		return result;
	}

	private interface ThrowingIntSupplier {
		int getAsInt() throws IOException;
	}
}
