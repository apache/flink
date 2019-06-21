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

package org.apache.flink.table.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.delegation.Executor;

/**
 * Factory to create an implementation of {@link Executor} to use in a
 * {@link org.apache.flink.table.api.TableEnvironment}. The {@link org.apache.flink.table.api.TableEnvironment}
 * should use {@link #create()} method that does not bind to any particular environment,
 * whereas {@link org.apache.flink.table.api.scala.StreamTableEnvironment} should use
 * {@link #create(StreamExecutionEnvironment)} as it is always backed by some {@link StreamExecutionEnvironment}
 */
@Internal
public class ExecutorFactory {
	/**
	 * Creates a {@link StreamExecutor} that is backed by given {@link StreamExecutionEnvironment}.
	 *
	 * @param executionEnvironment a {@link StreamExecutionEnvironment} to use while executing Table programs.
	 * @return {@link StreamExecutor}
	 */
	public static Executor create(StreamExecutionEnvironment executionEnvironment) {
		return new StreamExecutor(executionEnvironment);
	}

	/**
	 * Creates a {@link StreamExecutor} that is backed by a default {@link StreamExecutionEnvironment}.
	 *
	 * @return {@link StreamExecutor}
	 */
	public static Executor create() {
		return new StreamExecutor(StreamExecutionEnvironment.getExecutionEnvironment());
	}

	private ExecutorFactory() {
	}
}
