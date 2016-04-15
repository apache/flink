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
			log.info("\n================================================================================"
					+ "\nTest {} is running."
					+ "\n--------------------------------------------------------------------------------"
					, description);
		}

		@Override
		public void succeeded(Description description) {
			log.info("\n--------------------------------------------------------------------------------"
					+ "\nTest {} successfully run."
					+ "\n================================================================================"
					, description);
		}

		@Override
		public void failed(Throwable e, Description description) {
			log.error("\n--------------------------------------------------------------------------------"
					 + "\nTest {} failed with:\n{}"
					 + "\n================================================================================"
					, description, ExceptionUtils.stringifyException(e));
		}
	};
}
