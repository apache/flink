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

import org.apache.flink.connectors.test.common.utils.SuccessException;

/**
 * Patterns for how source job is terminated.
 *
 * <p>Since we cannot assume whether the tested source is bounded or not (whether the source job would finish itself),
 * framework user has to provide a pattern of terminating the job.</p>
 */
public enum SourceJobTerminationPattern {

	/* Using new source API introduced in FLIP-27 and the source itself is bounded. */
	BOUNDED_SOURCE,

	/**
	 * Using {@link org.apache.flink.api.common.serialization.DeserializationSchema#isEndOfStream}
	 * to stop the source.
	 */
	DESERIALIZATION_SCHEMA,

	/**
	 * Using a record provided by testing framework to mark the end of stream. If this pattern is chosen, a map
	 * operator will be added between source and sink as a filter, and
	 * {@link SuccessException} will be thrown, which will lead to failure
	 * of the job.
	 */
	END_MARK_FILTERING,

	/**
	 * The framework has to forcibly kill the job when some conditions are fulfilled.
	 */
	FORCE_STOP

}
