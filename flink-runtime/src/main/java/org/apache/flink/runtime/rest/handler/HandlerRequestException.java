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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.util.FlinkException;

/**
 * Base class for all {@link HandlerRequest} related exceptions.
 */
public class HandlerRequestException extends FlinkException {

	private static final long serialVersionUID = 7310878739304006028L;

	public HandlerRequestException(String message) {
		super(message);
	}

	public HandlerRequestException(Throwable cause) {
		super(cause);
	}

	public HandlerRequestException(String message, Throwable cause) {
		super(message, cause);
	}
}
