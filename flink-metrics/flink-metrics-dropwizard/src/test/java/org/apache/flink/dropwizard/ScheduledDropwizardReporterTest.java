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

package org.apache.flink.dropwizard;

import com.codahale.metrics.ScheduledReporter;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class ScheduledDropwizardReporterTest {

	@Test
	public void testInvalidCharacterReplacement() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		ScheduledDropwizardReporter reporter = new ScheduledDropwizardReporter() {
			@Override
			public ScheduledReporter getReporter(Configuration config) {
				return null;
			}
		};

		Class<? extends ScheduledDropwizardReporter> clazz = reporter.getClass();

		Method method = clazz.getSuperclass().getDeclaredMethod("replaceInvalidChars", String.class);

		assertEquals("abc", method.invoke(reporter, "abc"));
		assertEquals("a--b-c-", method.invoke(reporter, "a..b.c."));
		assertEquals("ab-c", method.invoke(reporter, "a\"b.c"));
	}
}
