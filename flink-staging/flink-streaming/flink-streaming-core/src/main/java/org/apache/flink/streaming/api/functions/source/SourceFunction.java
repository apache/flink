/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

/**
 * Interface for a stream data source.
 *
 * <p>Sources implementing this specific interface are executed with
 * parallelism 1. To execute your sources in parallel
 * see {@link ParallelSourceFunction}.</p>
 *
 * @param <OUT> The type of the records produced by this source.
 */
public interface SourceFunction<OUT> extends Function, Serializable {

	/**
	 * Main work method of the source. This function is invoked at the beginning of the
	 * source's life and is expected to produce its data py "pushing" the records into
	 * the given collector.
	 *
	 * @param collector The collector that forwards records to the source's consumers.
	 *
	 * @throws Exception Throwing any type of exception will cause the source to be considered
	 *                   failed. When fault tolerance is enabled, recovery will be triggered,
	 *                   which may create a new instance of this source.
	 */
	public void run(Collector<OUT> collector) throws Exception;

	/**
	 * This method signals the source function to cancel its operation
	 * The method is called by the framework if the task is to be aborted prematurely.
	 * This happens when the user cancels the job, or when the task is canceled as
	 * part of a program failure and cleanup.
	 */
	public void cancel();
}
