/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.testutils;

import org.apache.flink.util.function.RunnableWithException;

/**
 * Enables to run code with a {@link SecurityManager}.
 */
public class SecurityManagerContext implements AutoCloseable {

	private final SecurityManager previousSecurityManager;

	public SecurityManagerContext(final SecurityManager newSecurityManager) {
		this.previousSecurityManager = System.getSecurityManager();
		System.setSecurityManager(newSecurityManager);
	}

	public static void runWithSecurityManager(final SecurityManager securityManager, final RunnableWithException runnable) {
		try (SecurityManagerContext ignored = new SecurityManagerContext(securityManager)) {
			try {
				runnable.run();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void close() {
		System.setSecurityManager(previousSecurityManager);
	}
}
