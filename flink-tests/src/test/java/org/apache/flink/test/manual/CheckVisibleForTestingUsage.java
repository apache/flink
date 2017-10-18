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

package org.apache.flink.test.manual;

import org.apache.flink.annotation.VisibleForTesting;

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.MemberUsageScanner;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * This test check the methods are annotated with @VisibleForTesting. But still was called from the class
 * which does not belong to the tests. These methods should only be called from tests.
 */
public class CheckVisibleForTestingUsage {

	@Test
	public void testCheckVisibleForTesting() throws Exception {
		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder()
			.useParallelExecutor(Runtime.getRuntime().availableProcessors())
			.addUrls(ClasspathHelper.forPackage("org.apache.flink.core"))
			.addScanners(
				new MemberUsageScanner(),
				new MethodAnnotationsScanner());

		final Reflections reflections = new Reflections(configurationBuilder);

		Set<Method> methods = reflections.getMethodsAnnotatedWith(VisibleForTesting.class);

		for (Method method : methods) {
			Set<Member> usages = reflections.getMethodUsage(method);
			for (Member member : usages) {
				if (member instanceof Method) {
					Method visibleForTestingUsageScope = (Method) member;
					if (!visibleForTestingUsageScope.isAnnotationPresent(Test.class)) {
						assertEquals("Unexpected calls: " + visibleForTestingUsageScope.getDeclaringClass() + "#" + visibleForTestingUsageScope.getName(),
							"Only Suggest used in tests.");
					}
				}
			}
		}
	}
}
