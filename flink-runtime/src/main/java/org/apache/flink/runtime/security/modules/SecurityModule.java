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

package org.apache.flink.runtime.security.modules;

import java.security.GeneralSecurityException;

/**
 * An installable security module.
 */
public interface SecurityModule {

	/**
	 * Install the security module.
	 *
	 * @throws SecurityInstallException if the security module couldn't be installed.
	 */
	void install() throws SecurityInstallException;

	/**
	 * Uninstall the security module.
	 *
	 * @throws SecurityInstallException if the security module couldn't be uninstalled.
	 * @throws UnsupportedOperationException if the security module doesn't support uninstallation.
	 */
	void uninstall() throws SecurityInstallException;

	/**
	 * Indicates a problem with installing or uninstalling a security module.
	 */
	class SecurityInstallException extends GeneralSecurityException {
		private static final long serialVersionUID = 1L;

		public SecurityInstallException(String msg) {
			super(msg);
		}

		public SecurityInstallException(String msg, Throwable cause) {
			super(msg, cause);
		}
	}
}
