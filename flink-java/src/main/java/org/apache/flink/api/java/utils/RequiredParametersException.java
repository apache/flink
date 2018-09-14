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

package org.apache.flink.api.java.utils;

import org.apache.flink.annotation.PublicEvolving;

import java.util.LinkedList;
import java.util.List;

/**
 * Exception which is thrown if validation of {@link RequiredParameters} fails.
 */
@PublicEvolving
public class RequiredParametersException extends Exception {

	private List<String> missingArguments;

	public RequiredParametersException() {
		super();
	}

	public RequiredParametersException(String message, List<String> missingArguments) {
		super(message);
		this.missingArguments = missingArguments;
	}

	public RequiredParametersException(String message) {
		super(message);
	}

	public RequiredParametersException(String message, Throwable cause) {
		super(message, cause);
	}

	public RequiredParametersException(Throwable cause) {
		super(cause);
	}

	public List<String> getMissingArguments() {
		if (missingArguments == null) {
			return new LinkedList<>();
		} else {
			return this.missingArguments;
		}
	}
}
