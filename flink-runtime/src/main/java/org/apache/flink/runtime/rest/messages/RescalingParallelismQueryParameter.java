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

package org.apache.flink.runtime.rest.messages;

/**
 * Parallelism for the rescaling of jobs specified as a {@link MessageQueryParameter}.
 */
public class RescalingParallelismQueryParameter extends MessageQueryParameter<Integer> {

	public static final String KEY = "parallelism";

	public RescalingParallelismQueryParameter() {
		super(KEY, MessageParameterRequisiteness.MANDATORY);
	}

	@Override
	public Integer convertStringToValue(String value) {
		return Integer.valueOf(value);
	}

	@Override
	public String convertValueToString(Integer value) {
		return value.toString();
	}
}
