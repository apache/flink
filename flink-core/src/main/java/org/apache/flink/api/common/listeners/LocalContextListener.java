/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.listeners;

import org.apache.flink.api.common.ExecutionConfig;

/**
 * A user supplied call-back that allows intercepting tasks starting for the current {@link ClassLoader} on the local
 * JVM.
 */
public interface LocalContextListener {
	/**
	 * Called before any tasks are started from the current {@link Thread#getContextClassLoader()}. May be called
	 * more than once as long as there has been a matching call to {@link #closeContext(ExecutionConfig)} before a second call to
	 * {@link #openContext(ExecutionConfig)}.
	 *
	 * @param executionConfig the execution configuration.
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	default void openContext(ExecutionConfig executionConfig) throws Exception {
	}

	/**
	 * Called after any tasks running in the current {@link Thread#getContextClassLoader()} have completed.
	 * This method will only be invoked if a call to {@link #openContext(ExecutionConfig)} was
	 * attempted, and will be invoked irrespective of whether the call to {@link #openContext(ExecutionConfig)} terminated
	 * normally or exceptionally. Use this method to release any ClassLoader scoped resources that have been pooled
	 * across the stages of the topology.
	 *
	 * @param executionConfig the execution configuration.
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	default void closeContext(ExecutionConfig executionConfig) throws Exception {
	}

	/**
	 * Called before a thread starts executing a task.
	 *
	 * @param executionConfig the execution configuration.
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	default void openThread(ExecutionConfig executionConfig) throws Exception {
	}

	/**
	 * Called after a thread has finished executing a task.
	 * This method will only be invoked if a call to {@link #openThread(ExecutionConfig)} was
	 * attempted, and will be invoked irrespective of whether the call to {@link #openContext(ExecutionConfig)} terminated
	 * normally or exceptionally. Use this method to release any ClassLoader scoped resources that have been pooled
	 * across the stages of the topology.
	 *
	 * @param executionConfig the execution configuration.
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	default void closeThread(ExecutionConfig executionConfig) throws Exception {
	}

}
