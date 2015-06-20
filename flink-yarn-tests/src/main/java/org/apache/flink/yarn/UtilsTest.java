/**
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
package org.apache.flink.yarn;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UtilsTest {
	private static final Logger LOG = LoggerFactory.getLogger(UtilsTest.class);

	@Test
	public void testUberjarLocator() {
		File dir = YarnTestBase.findFile(".", new YarnTestBase.RootDirFilenameFilter());
		Assert.assertNotNull(dir);
		Assert.assertTrue(dir.getName().endsWith(".jar"));
		dir = dir.getParentFile().getParentFile(); // from uberjar to lib to root
		Assert.assertTrue(dir.exists());
		Assert.assertTrue(dir.isDirectory());
		List<String> files = Arrays.asList(dir.list());
		Assert.assertTrue(files.contains("lib"));
		Assert.assertTrue(files.contains("bin"));
		Assert.assertTrue(files.contains("conf"));
	}


	//
	// --------------- Tools to test if a certain string has been logged with Log4j. -------------
	// See :  http://stackoverflow.com/questions/3717402/how-to-test-w-junit-that-warning-was-logged-w-log4j
	//
	private static TestAppender testAppender;
	public static void addTestAppender(Class target, Level level) {
		testAppender = new TestAppender();
		testAppender.setThreshold(level);
		org.apache.log4j.Logger lg = org.apache.log4j.Logger.getLogger(target);
		lg.setLevel(level);
		lg.addAppender(testAppender);
		//org.apache.log4j.Logger.getRootLogger().addAppender(testAppender);
	}

	public static void checkForLogString(String expected) {
		if(testAppender == null) {
			throw new NullPointerException("Initialize it first");
		}
		LoggingEvent found = null;
		for(LoggingEvent event: testAppender.events) {
			if(event.getMessage().toString().contains(expected)) {
				found = event;
				break;
			}
		}
		if(found != null) {
			LOG.info("Found expected string '"+expected+"' in log message "+found);
			return;
		}
		Assert.fail("Unable to find expected string '" + expected + "' in log messages");
	}

	public static class TestAppender extends AppenderSkeleton {
		public List<LoggingEvent> events = new ArrayList<LoggingEvent>();
		public void close() {}
		public boolean requiresLayout() {return false;}
		@Override
		protected void append(LoggingEvent event) {
			events.add(event);
		}
	}
}

