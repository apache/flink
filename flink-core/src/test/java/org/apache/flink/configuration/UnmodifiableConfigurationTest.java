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

package org.apache.flink.configuration;


import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.lang.reflect.Method;

/**
 * This class verifies that the Unmodifiable Configuration class overrides all setter methods in
 * Configuration.
 */
public class UnmodifiableConfigurationTest {

	private static Configuration pc = new Configuration();
	private static UnmodifiableConfiguration unConf = new UnmodifiableConfiguration(pc);
	private static Class clazz = unConf.getClass();

	@Test
	public void testOverride() throws Exception{
		for(Method m : clazz.getMethods()){
			if(m.getName().indexOf("set") == 0 || m.getName().indexOf("add") == 0 ) {
				assertEquals(clazz, m.getDeclaringClass());
			}
		}
	}
}
