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

import org.apache.flink.util.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

/*
 * Process-wide security context object which initializes UGI with appropriate security credentials and also it
 * creates in-memory JAAS configuration object which will serve appropriate ApplicationConfigurationEntry for the
 * connector login module implementation that authenticates Kerberos identity using SASL/JAAS based mechanism.
 */
class HadoopSecurityContext implements SecurityContext {

	private UserGroupInformation ugi;

	HadoopSecurityContext(UserGroupInformation ugi) {
		this.ugi = Preconditions.checkNotNull(ugi, "UGI passed cannot be null");
	}

	public <T> T runSecured(final Callable<T> securedCallable) throws Exception {
		return ugi.doAs(new PrivilegedExceptionAction<T>() {
			@Override
			public T run() throws Exception {
				return securedCallable.call();
			}
		});
	}

}
