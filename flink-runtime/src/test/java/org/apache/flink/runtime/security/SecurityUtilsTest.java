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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.OperatingSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SecurityUtils}.
 */
public class SecurityUtilsTest {

	@AfterClass
	public static void afterClass() {
		SecurityUtils.clearContext();
		System.setProperty(SecurityUtils.JAVA_SECURITY_AUTH_LOGIN_CONFIG, "");
	}

	@Test
	public void testCreateInsecureHadoopCtx() {
		SecurityUtils.SecurityConfiguration sc = new SecurityUtils.SecurityConfiguration(new Configuration());
		try {
			SecurityUtils.install(sc);
			assertEquals(UserGroupInformation.getLoginUser().getUserName(), getOSUserName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testInvalidUGIContext() {
		try {
			new HadoopSecurityContext(null);
		} catch (RuntimeException re) {
			assertEquals("UGI passed cannot be null",re.getMessage());
		}
	}

	@Test
	/**
	 * The Jaas configuration file provided should not be overridden.
	 */
	public void testJaasPropertyOverride() throws Exception {
		String confFile = "jaas.conf";
		System.setProperty(SecurityUtils.JAVA_SECURITY_AUTH_LOGIN_CONFIG, confFile);

		SecurityUtils.install(new SecurityUtils.SecurityConfiguration(new Configuration()));

		Assert.assertEquals(
			confFile,
			System.getProperty(SecurityUtils.JAVA_SECURITY_AUTH_LOGIN_CONFIG));
	}


	private String getOSUserName() throws Exception {
		String userName = "";
		OperatingSystem os = OperatingSystem.getCurrentOperatingSystem();
		String className;
		String methodName;

		switch(os) {
			case LINUX:
			case MAC_OS:
				className = "com.sun.security.auth.module.UnixSystem";
				methodName = "getUsername";
				break;
			case WINDOWS:
				className = "com.sun.security.auth.module.NTSystem";
				methodName = "getName";
				break;
			case SOLARIS:
				className = "com.sun.security.auth.module.SolarisSystem";
				methodName = "getUsername";
				break;
			case FREE_BSD:
			case UNKNOWN:
			default:
				className = null;
				methodName = null;
		}

		if( className != null ){
			Class<?> c = Class.forName( className );
			Method method = c.getDeclaredMethod( methodName );
			Object o = c.newInstance();
			userName = (String) method.invoke( o );
		}
		return userName;
	}
}
