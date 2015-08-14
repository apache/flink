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

package org.apache.flink.util;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds automatic test name logging. Every test which wants to log which test is currently
 * executed and why it failed, simply has to extend this class.
 */
public class TestLogger {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	@Rule
	public TestRule watchman = new TestWatcher() {

		@Override
		public void starting(Description description) {
			if (log.isInfoEnabled()) {
				String logText = "Test " + description + " is running.";
				String headRuler = org.apache.commons.lang3.StringUtils.repeat("=", logText.length());
				String bottomRuler = org.apache.commons.lang3.StringUtils.repeat("-", logText.length());

				log.info(headRuler);
				log.info(logText, description);
				log.info(bottomRuler);
			}
		}

		@Override
		public void succeeded(Description description) {
			if (log.isInfoEnabled()) {
				String logText = "Test " + description + " successfully run.";
				String bottomRuler = org.apache.commons.lang3.StringUtils.repeat("=", logText.length());
				String headRuler = org.apache.commons.lang3.StringUtils.repeat("-", logText.length());

				log.info(headRuler);
				log.info(logText, description);
				log.info(bottomRuler);
			}
		}

		@Override
		public void failed(Throwable e, Description description) {
			if (log.isErrorEnabled()) {
				String testDescription = "Test " + description + " failed with:";
				String testException = StringUtils.stringifyException(e);
				String logText = testDescription + "\n" + testException;
				String bottomRuler = org.apache.commons.lang3.StringUtils.repeat("=", testDescription.length());
				String headRuler = org.apache.commons.lang3.StringUtils.repeat("-", testDescription.length());

				log.error(headRuler);
				log.error(logText);
				log.error(bottomRuler);
			}
		}
	};
}
