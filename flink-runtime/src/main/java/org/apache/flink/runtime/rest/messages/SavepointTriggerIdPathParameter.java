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

import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerId;

/**
 * Path parameter identifying savepoint trigger requests.
 */
public class SavepointTriggerIdPathParameter extends MessagePathParameter<SavepointTriggerId> {

	public static final String KEY = "savepointtriggerid";

	public SavepointTriggerIdPathParameter() {
		super(KEY);
	}

	@Override
	protected SavepointTriggerId convertFromString(String value) {
		return SavepointTriggerId.fromHexString(value);
	}

	@Override
	protected String convertToString(SavepointTriggerId value) {
		return value.toString();
	}
}
