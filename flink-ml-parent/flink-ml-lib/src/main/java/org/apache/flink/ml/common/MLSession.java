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
 * MLSession hold the execution environment and others session shared variable.
 *
 * <P>In machine learning, we find that a single execution context is convenient for the user
 * and thus we created the concept of machine learning session to hold the execution environment.
 * For this reason, the batch tables using in machine learning should be created by the unique
 * {@link BatchTableEnvironment}, and the stream tables using in machine learning should be created
 * by the unique {@link StreamTableEnvironment}
 */
public class MLSession {
	private static ExecutionEnvironment env;
	private static StreamExecutionEnvironment streamEnv;
	private static BatchTableEnvironment batchTableEnv;
	private static StreamTableEnvironment streamTableEnv;

	/**
	 * Factory to create {@link ExecutionEnvironment}.
	 *
	 * <P>We create the {@link ExecutionEnvironment} using default flink configuration
	 * when user has not been set the {@link ExecutionEnvironment}
	 *
	 * @return the batch {@link ExecutionEnvironment}
	 * @see MLSession#setExecutionEnvironment(ExecutionEnvironment)
	 */
	public static synchronized ExecutionEnvironment getExecutionEnvironment() {
		if (null == env) {
			env = ExecutionEnvironment.getExecutionEnvironment();
		}
		return env;
	}

	/**
	 * Factory to create {@link StreamExecutionEnvironment}.
	 *
	 * <P>We create the {@link StreamExecutionEnvironment} using default flink configuration
	 * when user has not been set the {@link StreamExecutionEnvironment}
	 *
	 * @return the {@link StreamExecutionEnvironment}
	 * @see MLSession#setStreamExecutionEnvironment(StreamExecutionEnvironment)
	 */
	public static synchronized StreamExecutionEnvironment getStreamExecutionEnvironment() {
		if (null == streamEnv) {
			streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		}
		return streamEnv;
	}

	/**
	 * Factory to create {@link BatchTableEnvironment}.
	 *
	 * <P>We create the {@link BatchTableEnvironment} using {@link MLSession#getExecutionEnvironment()}
	 * when user has not been set the {@link BatchTableEnvironment}
	 *
	 * @return the {@link BatchTableEnvironment}
	 * @see MLSession#setTableEnvironment(TableEnvironment, Table)
	 */
	public static synchronized BatchTableEnvironment getBatchTableEnvironment() {
		if (null == batchTableEnv) {
			batchTableEnv = BatchTableEnvironment.create(getExecutionEnvironment());
		}
		return batchTableEnv;
	}

	/**
	 * Factory to create {@link StreamTableEnvironment}.
	 *
	 * <P>We create the {@link StreamTableEnvironment} using {@link MLSession#getExecutionEnvironment()}
	 * when user has not been set the {@link StreamTableEnvironment}
	 *
	 * @return the {@link StreamTableEnvironment}
	 * @see MLSession#setTableEnvironment(TableEnvironment, Table)
	 */
	public static synchronized StreamTableEnvironment getStreamTableEnvironment() {
		if (null == streamTableEnv) {
			streamTableEnv = StreamTableEnvironment.create(getStreamExecutionEnvironment());
		}
		return streamTableEnv;
	}

	/**
	 * Set the session shared ExecutionEnvironment.
	 *
	 * <P>The ExecutionEnvironment should be set only once.
	 *
	 * @param env the ExecutionEnvironment
	 */
	public static synchronized void setExecutionEnvironment(ExecutionEnvironment env) {
		assert env != null;

		if (MLSession.env != null && MLSession.env != env) {
			throw new RuntimeException("There should be only one batch execution environment in a session");
		}

		MLSession.env = env;
	}

	/**
	 * Set the session shared StreamExecutionEnvironment.
	 *
	 * <P>The StreamExecutionEnvironment should be set only once.
	 *
	 * @param env the StreamExecutionEnvironment
	 */
	public static synchronized void setStreamExecutionEnvironment(StreamExecutionEnvironment env) {
		assert env != null;

		if (MLSession.streamEnv != null && MLSession.streamEnv != env) {
			throw new RuntimeException("There should be only one stream execution environment in a session");
		}

		MLSession.streamEnv = env;
	}

	/**
	 * Set the session shared TableEnvironment.
	 *
	 * <p>The TableEnvironment should be set only once.
	 *
	 * <p>We also set the {@link ExecutionEnvironment} or {@link StreamExecutionEnvironment} accordingly.
	 *
	 * @param tEnv the TableEnvironment
	 * @param table the table for get {@link ExecutionEnvironment} for batch
	 */
	public static synchronized void setTableEnvironment(TableEnvironment tEnv, Table table) {
		assert tEnv != null;

		if (tEnv instanceof StreamTableEnvironment) {
			if ((streamTableEnv != null && tEnv != streamTableEnv)
				|| (MLSession.streamEnv != null
				&& MLSession.streamEnv != ((StreamTableEnvironmentImpl) tEnv).execEnv())
			) {
				throw new RuntimeException("There should be only one stream table environment in a session");
			} else {
				streamTableEnv = (StreamTableEnvironment) tEnv;

				if (MLSession.streamEnv == null) {
					MLSession.streamEnv = ((StreamTableEnvironmentImpl) streamTableEnv).execEnv();
				}
			}
		} else {
			ExecutionEnvironment env =
				((BatchTableEnvironment) tEnv).toDataSet(table, Row.class).getExecutionEnvironment();
			if ((batchTableEnv != null && tEnv != batchTableEnv)
				|| (MLSession.env != null && MLSession.env != env)) {
				throw new RuntimeException("There should be only one batch table environment in a session");
			} else {
				batchTableEnv = (BatchTableEnvironment) tEnv;

				if (MLSession.env == null) {
					MLSession.env = env;
				}
			}
		}
	}

}

