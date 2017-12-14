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

package org.apache.flink.queryablestate.exceptions;

import org.apache.flink.annotation.Internal;

/**
 * Exception to fail Future if the Task Manager on which the
 * {@code Client Proxy} is running on, does not know the active
 * Job Manager.
 */
@Internal
public class UnknownJobManagerException extends Exception {

	private static final long serialVersionUID = 9092442511708951209L;

	public UnknownJobManagerException() {
		super("Unknown JobManager. Either the JobManager has not registered yet or has lost leadership.");
	}
}
