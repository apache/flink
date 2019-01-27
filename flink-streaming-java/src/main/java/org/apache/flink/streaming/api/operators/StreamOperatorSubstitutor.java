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

package org.apache.flink.streaming.api.operators;

/**
 * Basic interface for stream operator substitutes. It is transferred to the streamTask by
 * serialization, and produce an actual stream operator to the streamTask, who uses the actual
 * stream operator to run.
 *
 * @param <OUT> output type of the actual stream operator
 */
public interface StreamOperatorSubstitutor<OUT> {

	/**
	 * Produces the actual stream operator.
	 *
	 * @param userCodeClassLoader the user code class loader to use.
	 * @return the actual stream operator created on {@code StreamTask}.
	 */
	StreamOperator<OUT> getActualStreamOperator(ClassLoader userCodeClassLoader);
}
