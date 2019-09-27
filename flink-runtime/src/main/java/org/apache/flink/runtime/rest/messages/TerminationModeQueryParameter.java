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
 * Termination mode query parameter.
 */
public class TerminationModeQueryParameter extends MessageQueryParameter<TerminationModeQueryParameter.TerminationMode> {

	private static final String key = "mode";

	public TerminationModeQueryParameter() {
		super(key, MessageParameterRequisiteness.OPTIONAL);
	}

	@Override
	public TerminationMode convertStringToValue(String value) {
		return TerminationMode.valueOf(value.toUpperCase());
	}

	@Override
	public String convertValueToString(TerminationMode value) {
		return value.name().toLowerCase();
	}

	@Override
	public String getDescription() {
		return "String value that specifies the termination mode. The only supported value is: \"" +
			TerminationMode.CANCEL.name().toLowerCase() + "\".";
	}

	/**
	 * Termination mode.
	 */
	public enum TerminationMode {
		CANCEL,

		/**
		 * @deprecated Please use the "stop" command instead.
		 */
		STOP
	}
}
