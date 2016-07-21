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

import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.Test;

import javax.security.auth.login.AppConfigurationEntry;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JaasConfiguration}.
 */
public class JaasConfigurationTest {

	@Test
	public void testInvalidKerberosParams() {
		String keytab = "user.keytab";
		String principal = null;
		try {
			new JaasConfiguration(keytab, principal);
		} catch(RuntimeException re) {
			assertEquals("Both keytab and principal are required and cannot be empty",re.getMessage());
		}
	}

	@Test
	public void testDefaultAceEntry() {
		JaasConfiguration conf = new JaasConfiguration(null,null);
		javax.security.auth.login.Configuration.setConfiguration(conf);
		final AppConfigurationEntry[] entry = conf.getAppConfigurationEntry("test");
		AppConfigurationEntry ace = entry[0];
		assertEquals(ace.getLoginModuleName(), KerberosUtil.getKrb5LoginModuleName());
	}
}