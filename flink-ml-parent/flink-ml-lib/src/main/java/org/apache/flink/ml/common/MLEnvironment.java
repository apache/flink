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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * The MLEnvironment stores the necessary context in Flink.
 * Each MLEnvironment will be associated with a unique ID.
 * The operations associated with the same MLEnvironment ID
 * will share the same Flink job context.
 *
 * <p>Both MLEnvironment ID and MLEnvironment can only be retrieved from MLEnvironmentFactory.
 *
 * @see ExecutionEnvironment
 * @see StreamExecutionEnvironment
 * @see BatchTableEnvironment
 * @see StreamTableEnvironment
 */
public class MLEnvironment {
	private ExecutionEnvironment env;
	private StreamExecutionEnvironment streamEnv;
	private BatchTableEnvironment batchTableEnv;
	private StreamTableEnvironment streamTableEnv;

	/**
	 * Construct with null that the class can load the environment in the `get` method.
	 */
	public MLEnvironment() {
		this(null, null, null, null);
	}

	/**
	 * Construct with the batch environment and the the batch table environment given by user.
	 *
	 * <p>The env can be null which will be loaded in the `get` method.
	 *
	 * @param batchEnv the ExecutionEnvironment
	 * @param batchTableEnv the BatchTableEnvironment
	 */
	public MLEnvironment(
		ExecutionEnvironment batchEnv,
		BatchTableEnvironment batchTableEnv) {
		this(batchEnv, batchTableEnv, null, null);
	}

	/**
	 * Construct with the stream environment and the the stream table environment given by user.
	 *
	 * <p>The env can be null which will be loaded in the `get` method.
	 *
	 * @param streamEnv the StreamExecutionEnvironment
	 * @param streamTableEnv the StreamTableEnvironment
	 */
	public MLEnvironment(
		StreamExecutionEnvironment streamEnv,
		StreamTableEnvironment streamTableEnv) {
		this(null, null, streamEnv, streamTableEnv);
	}

	/**
	 * Construct with env given by user.
	 *
	 * <p>The env can be null which will be loaded in the `get` method.
	 *
	 * @param batchEnv the ExecutionEnvironment
	 * @param batchTableEnv the BatchTableEnvironment
	 * @param streamEnv the StreamExecutionEnvironment
	 * @param streamTableEnv the StreamTableEnvironment
	 */
	public MLEnvironment(
		ExecutionEnvironment batchEnv,
		BatchTableEnvironment batchTableEnv,
		StreamExecutionEnvironment streamEnv,
		StreamTableEnvironment streamTableEnv) {
		this.env = batchEnv;
		this.batchTableEnv = batchTableEnv;
		this.streamEnv = streamEnv;
		this.streamTableEnv = streamTableEnv;
	}

	/**
	 * Get the ExecutionEnvironment.
	 * if the ExecutionEnvironment has not been set, it initial the ExecutionEnvironment
	 * with default Configuration.
	 *
	 * @return the batch {@link ExecutionEnvironment}
	 */
	public ExecutionEnvironment getExecutionEnvironment() {
		if (null == env) {
			env = ExecutionEnvironment.getExecutionEnvironment();
		}
		return env;
	}

	/**
	 * Get the StreamExecutionEnvironment.
	 * if the StreamExecutionEnvironment has not been set, it initial the StreamExecutionEnvironment
	 * with default Configuration.
	 *
	 * @return the {@link StreamExecutionEnvironment}
	 */
	public StreamExecutionEnvironment getStreamExecutionEnvironment() {
		if (null == streamEnv) {
			streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		}
		return streamEnv;
	}

	/**
	 * Get the BatchTableEnvironment.
	 * if the BatchTableEnvironment has not been set, it initial the BatchTableEnvironment
	 * with default Configuration.
	 *
	 * @return the {@link BatchTableEnvironment}
	 */
	public BatchTableEnvironment getBatchTableEnvironment() {
		if (null == batchTableEnv) {
			batchTableEnv = BatchTableEnvironment.create(getExecutionEnvironment());
		}
		return batchTableEnv;
	}

	/**
	 * Get the StreamTableEnvironment.
	 * if the StreamTableEnvironment has not been set, it initial the StreamTableEnvironment
	 * with default Configuration.
	 *
	 * @return the {@link StreamTableEnvironment}
	 */
	public StreamTableEnvironment getStreamTableEnvironment() {
		if (null == streamTableEnv) {
			streamTableEnv = StreamTableEnvironment.create(
					getStreamExecutionEnvironment(),
					EnvironmentSettings.newInstance().useOldPlanner().build());
		}
		return streamTableEnv;
	}
}

