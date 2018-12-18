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
 * Path parameter specifying the index of a subtask.
 */
public class SubtaskIndexPathParameter extends MessagePathParameter<Integer> {

	public static final String KEY = "subtaskindex";

	public SubtaskIndexPathParameter() {
		super(KEY);
	}

	@Override
	protected Integer convertFromString(final String value) throws ConversionException {
		final int subtaskIndex = Integer.parseInt(value);
		if (subtaskIndex >= 0) {
			return subtaskIndex;
		} else {
			throw new ConversionException("subtaskindex must be positive, was: " + subtaskIndex);
		}
	}

	@Override
	protected String convertToString(final Integer value) {
		return value.toString();
	}

	@Override
	public String getDescription() {
		return "Positive integer value that identifies a subtask.";
	}

}
