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

package org.apache.flink.connectors.test.common.external;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;

/**
 * Context of the test interacting with external system.
 *
 * <p>User need to provide some instances and information of the test to testing framework, including name of Flink
 * jobs, instance of tested source/sink and job termination pattern.</p>
 *
 * @param <T> Type of elements after deserialization by source and before serialization by sink
 */
public interface ExternalContext<T> extends Serializable {

	/**
	 * Get the name of the Flink job used for testing.
	 * @return Name of the Flink job
	 */
	String jobName();

	/**
	 * Create a new instance of the source. This will be invoked when the main function of the Flink job is invoked.
	 * @return A new instance of the source
	 */
	SourceFunction<T> createSource();

	/**
	 * Create a new instance of the sink. This will be invoked when the main function of the Flink job is invoked.
	 * @return A new instance of the sink
	 */
	SinkFunction<T> createSink();

	/**
	 * Get the termination pattern of the job. Check {@link SourceJobTerminationPattern}
	 * for more descriptions.
	 * @return Termination pattern of the Flink job
	 */
	SourceJobTerminationPattern sourceJobTerminationPattern();
}
