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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;

/**
 * MLEnvironment hold the execution environment.
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
	 * Get the ExecutionEnvironment.
	 * if the ExecutionEnvironment has not been set, it initial the ExecutionEnvironment
	 * with default Configuration.
	 *
	 * @return the batch {@link ExecutionEnvironment}
	 * @see MLEnvironment#setExecutionEnvironment(ExecutionEnvironment)
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
	 * @see MLEnvironment#setStreamExecutionEnvironment(StreamExecutionEnvironment)
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
	 * @see MLEnvironment#setTableEnvironment(TableEnvironment, Table)
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
	 * @see MLEnvironment#setTableEnvironment(TableEnvironment, Table)
	 */
	public StreamTableEnvironment getStreamTableEnvironment() {
		if (null == streamTableEnv) {
			streamTableEnv = StreamTableEnvironment.create(getStreamExecutionEnvironment());
		}
		return streamTableEnv;
	}

	/**
	 * Set the ExecutionEnvironment.
	 * The ExecutionEnvironment should be set only once.
	 *
	 * @param env the ExecutionEnvironment
	 */
	public void setExecutionEnvironment(ExecutionEnvironment env) {
		assert env != null;

		if (this.env != null && this.env != env) {
			throw new RuntimeException("There should be only one batch execution environment");
		}

		this.env = env;
	}

	/**
	 * Set the StreamExecutionEnvironment.
	 * The StreamExecutionEnvironment should be set only once.
	 *
	 * @param env the StreamExecutionEnvironment
	 */
	public void setStreamExecutionEnvironment(StreamExecutionEnvironment env) {
		assert env != null;

		if (this.streamEnv != null && this.streamEnv != env) {
			throw new RuntimeException("There should be only one stream execution environment");
		}

		this.streamEnv = env;
	}

	/**
	 * Set the TableEnvironment.
	 * The TableEnvironment should be set only once.
	 *
	 * <p>We also set the {@link ExecutionEnvironment} or {@link StreamExecutionEnvironment} accordingly.
	 *
	 * @param tEnv  the TableEnvironment
	 * @param table the table for get {@link ExecutionEnvironment} for batch
	 */
	public void setTableEnvironment(TableEnvironment tEnv, Table table) {
		assert tEnv != null;

		if (tEnv instanceof StreamTableEnvironment) {
			if ((streamTableEnv != null && tEnv != streamTableEnv)
				|| (this.streamEnv != null
				&& this.streamEnv != ((StreamTableEnvironmentImpl) tEnv).execEnv())
			) {
				throw new RuntimeException("There should be only one stream table environment");
			} else {
				streamTableEnv = (StreamTableEnvironment) tEnv;

				if (this.streamEnv == null) {
					this.streamEnv = ((StreamTableEnvironmentImpl) streamTableEnv).execEnv();
				}
			}
		} else {
			ExecutionEnvironment env =
				((BatchTableEnvironment) tEnv).toDataSet(table, Row.class).getExecutionEnvironment();
			if ((batchTableEnv != null && tEnv != batchTableEnv)
				|| (this.env != null && this.env != env)) {
				throw new RuntimeException("There should be only one batch table environment");
			} else {
				batchTableEnv = (BatchTableEnvironment) tEnv;

				if (this.env == null) {
					this.env = env;
				}
			}
		}
	}
}

