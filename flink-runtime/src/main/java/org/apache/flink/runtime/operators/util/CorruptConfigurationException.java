/**
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


package org.apache.flink.runtime.operators.util;

/**
 * Exception indicating that the parsed configuration was corrupt.
 * Corruption typically means missing or invalid values.
 * 
 */
public class CorruptConfigurationException extends RuntimeException
{
	private static final long serialVersionUID = 854450995262666207L;

	/**
	 * Creates a new exception with the given error message.
	 * 
	 * @param message The exception's message.
	 */
	public CorruptConfigurationException(String message) {
		super(message);
	}

	public CorruptConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
}
