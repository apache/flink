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

import static org.apache.flink.util.Preconditions.checkState;

/**
 * SecurityManager implementation that forbids and tracks calls to {@link System#exit(int)}.
 */
public class SystemExitTrackingSecurityManager extends SecurityManager {

	private int status;

	private int count;

	@Override
	public void checkPermission(final Permission perm) {

	}

	@Override
	public void checkPermission(final Permission perm, final Object context) {

	}

	@Override
	public synchronized void checkExit(final int status) {
		this.status = status;
		++count;
		throw new SecurityException("SystemExitTrackingSecurityManager is installed. JVM will not exit");
	}

	public synchronized int getStatus() {
		checkState(isSystemExitCalled());
		return status;
	}

	public synchronized boolean isSystemExitCalled() {
		return getCount() > 0;
	}

	public synchronized int getCount() {
		return count;
	}

}
