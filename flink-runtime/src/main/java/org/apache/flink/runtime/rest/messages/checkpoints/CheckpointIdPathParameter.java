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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;

/**
 * Path parameter for the checkpoint id of type {@link Long}.
 */
public class CheckpointIdPathParameter extends MessagePathParameter<Long> {

	public static final String KEY = "checkpointid";

	protected CheckpointIdPathParameter() {
		super(KEY);
	}

	@Override
	protected Long convertFromString(String value) throws ConversionException {
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException nfe) {
			throw new ConversionException("Could not parse long from " + value + '.', nfe);
		}
	}

	@Override
	protected String convertToString(Long value) {
		return value.toString();
	}
}
