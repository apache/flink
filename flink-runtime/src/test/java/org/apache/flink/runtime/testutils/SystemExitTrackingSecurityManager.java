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

import java.security.Permission;
import java.util.concurrent.CompletableFuture;

/**
 * {@link SecurityManager} implementation blocks calls to {@link System#exit(int)}. On the first
 * call to {@link System#exit(int)}, we complete the future that can be retrieved via {@link
 * #getSystemExitFuture()}.
 */
public class SystemExitTrackingSecurityManager extends SecurityManager {

	private final CompletableFuture<Integer> systemExitFuture = new CompletableFuture<>();

	@Override
	public void checkPermission(final Permission perm) {
	}

	@Override
	public void checkPermission(final Permission perm, final Object context) {
	}

	@Override
	public synchronized void checkExit(final int status) {
		systemExitFuture.complete(status);
		throw new SecurityException(
				"SystemExitTrackingSecurityManager is installed. JVM will not exit");
	}

	/**
	 * Returns a {@link CompletableFuture} that is completed with the exit code when {@link
	 * System#exit(int)} is called.
	 */
	public CompletableFuture<Integer> getSystemExitFuture() {
		return systemExitFuture;
	}
}
