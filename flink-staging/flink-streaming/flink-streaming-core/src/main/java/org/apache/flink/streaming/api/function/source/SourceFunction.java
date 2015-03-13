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

package org.apache.flink.streaming.api.function.source;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

/**
 * Interface for implementing user defined source functionality.
 *
 * <p>Sources implementing this specific interface are executed with
 * degree of parallelism 1. To execute your sources in parallel
 * see {@link ParallelSourceFunction}.</p>
 *
 * @param <OUT> Output type parameter.
 */
public interface SourceFunction<OUT> extends Function, Serializable {

	/**
	 * Function for standard source behaviour. This function is called only once
	 * thus to produce multiple outputs make sure to produce multiple records.
	 *
	 * @param collector Collector for passing output records
	 * @throws Exception
	 */
	public void run(Collector<OUT> collector) throws Exception;

	/**
	 * In case another vertex in topology fails this method is called before terminating
	 * the source. Make sure to free up any allocated resources here.
	 */
	public void cancel();
		
}
