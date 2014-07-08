/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.util;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;

import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.DelegatingConfiguration;


public class TestDelegatingConfiguration {

	/**
	 * http://stackoverflow.com/questions/22225663/checking-in-a-unit-test-whether-all-methods-are-delegated
	 */
	@Test
	public void testIfDelegatesImplementAllMethods() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		Comparator<Method> methodComparator = new Comparator<Method>() {
			@Override
			public int compare(Method o1, Method o2) {
				String o1Str = o1.getName() + typeParamToString(o1.getParameterTypes());
				String o2Str = o2.getName() + typeParamToString(o2.getParameterTypes());
				return o1Str.compareTo( o2Str ); 
			}

			private String typeParamToString(Class<?>[] classes) {
				String str = "";
				for(Object t : classes) {
					str += t.toString();
				}
				return str;
			}
		};
		
		// For each method in the Configuration class...
		Method[] confMethods = Configuration.class.getDeclaredMethods();
		Method[] delegateMethods = DelegatingConfiguration.class.getDeclaredMethods();
		Arrays.sort(confMethods, methodComparator);
		Arrays.sort(delegateMethods, methodComparator);
		match : for (Method configurationMethod : confMethods) {
			boolean hasMethod = false;
			if(!Modifier.isPublic(configurationMethod.getModifiers()) ) {
				continue;
			}
			// Find matching method in wrapper class and call it
			mismatch: for (Method wrapperMethod : delegateMethods) {
				if (configurationMethod.getName().equals(wrapperMethod.getName())) {
					
					// Get parameters for method
					Class<?>[] wrapperMethodParams = wrapperMethod.getParameterTypes();
					Class<?>[] configMethodParams = configurationMethod.getParameterTypes();
					if(wrapperMethodParams.length != configMethodParams.length) {
						System.err.println("Length");
						break mismatch;
					}
					for(int i = 0; i < wrapperMethodParams.length; i++) {
						if(wrapperMethodParams[i] != configMethodParams[i]) {
							break mismatch;
						}
					}
					hasMethod = true;
					break match;
				}
			}
			assertTrue("Foo method '" + configurationMethod.getName() + "' has not been wrapped correctly in DelegatingConfiguration wrapper", hasMethod);
		}
	}
}
