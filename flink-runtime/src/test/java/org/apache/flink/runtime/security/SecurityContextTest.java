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

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SecurityContext}.
 */
public class SecurityContextTest {

	@Test
	public void testCreateInsecureHadoopCtx() {
		SecurityContext.SecurityConfiguration sc = new SecurityContext.SecurityConfiguration();
		try {
			SecurityContext.install(sc);
			assertEquals(UserGroupInformation.getLoginUser().getUserName(),getOSUserName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testInvalidUGIContext() {
		try {
			new SecurityContext(null);
		} catch (RuntimeException re) {
			assertEquals("UGI passed cannot be null",re.getMessage());
		}
	}


	private String getOSUserName() throws Exception {
		String userName = "";
		String osName = System.getProperty( "os.name" ).toLowerCase();
		String className = null;

		if( osName.contains( "windows" ) ){
			className = "com.sun.security.auth.module.NTSystem";
		}
		else if( osName.contains( "linux" ) ){
			className = "com.sun.security.auth.module.UnixSystem";
		}
		else if( osName.contains( "solaris" ) || osName.contains( "sunos" ) ){
			className = "com.sun.security.auth.module.SolarisSystem";
		}

		if( className != null ){
			Class<?> c = Class.forName( className );
			Method method = c.getDeclaredMethod( "getUsername" );
			Object o = c.newInstance();
			userName = (String) method.invoke( o );
		}
		return userName;
	}
}
