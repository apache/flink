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

package org.apache.flink.runtime.security;

import java.util.List;

/**
 * Exception for not finding suitable security factories.
 */
public class NoMatchSecurityFactoryException extends RuntimeException {

	/**
	 * Exception for not finding suitable security factories.
	 *
	 * @param message message that indicates the current matching step
	 * @param factoryClassCanonicalName required factory class
	 * @param matchingFactories all found factories
	 * @param cause the cause
	 */
	public NoMatchSecurityFactoryException(
		String message,
		String factoryClassCanonicalName,
		List<?> matchingFactories,
		Throwable cause) {
		super("Could not find a suitable security factory for '"
			+ factoryClassCanonicalName + "' in the classpath. all matching factories: "
			+ matchingFactories + ". Reason: " + message, cause);
	}

	/**
	 * Exception for not finding suitable security factories.
	 *
	 * @param message message that indicates the current matching step
	 * @param factoryClassCanonicalName required factory class
	 * @param matchingFactories all found factories
	 */
	public NoMatchSecurityFactoryException(
		String message,
		String factoryClassCanonicalName,
		List<?> matchingFactories) {
		this(message, factoryClassCanonicalName, matchingFactories, null);
	}
}
