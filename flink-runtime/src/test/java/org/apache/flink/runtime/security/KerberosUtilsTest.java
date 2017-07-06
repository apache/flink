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

import org.junit.Test;

import javax.security.auth.login.AppConfigurationEntry;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link KerberosUtils}.
 */
public class KerberosUtilsTest {

	@Test
	public void testTicketCacheEntry() {
		AppConfigurationEntry entry = KerberosUtils.ticketCacheEntry();
		assertNotNull(entry);
	}

	@Test
	public void testKeytabEntry() {
		String keytab = "user.keytab";
		String principal = "user";
		AppConfigurationEntry entry = KerberosUtils.keytabEntry(keytab, principal);
		assertNotNull(entry);
	}

}
