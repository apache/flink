/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.testutils.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Utility for auditing logged messages.
 *
 * <p>Implementation note: Make sure to not expose log4j dependencies in the interface of this class to ease updates
 * in logging infrastructure.
 */
public class TestLoggerResource extends ExternalResource {
	private static final LoggerContext LOGGER_CONTEXT = (LoggerContext) LogManager.getContext(false);

	private final String loggerName;
	private final org.slf4j.event.Level level;

	private ConcurrentLinkedQueue<String> loggingEvents;

	public TestLoggerResource(Class<?> clazz, org.slf4j.event.Level level) {
		this.loggerName = clazz.getCanonicalName();
		this.level = level;
	}

	public List<String> getMessages() {
			return new ArrayList<>(loggingEvents);
	}

	@Override
	protected void before() throws Throwable {
		loggingEvents = new ConcurrentLinkedQueue<>();

		Appender testAppender = new AbstractAppender("test-appender", null, null, false) {
			@Override
			public void append(LogEvent event) {
				loggingEvents.add(event.getMessage().getFormattedMessage());
			}
		};
		testAppender.start();

		AppenderRef appenderRef = AppenderRef.createAppenderRef(testAppender.getName(), null, null);
		LoggerConfig logger = LoggerConfig.createLogger(
			false,
			Level.getLevel(level.name()),
			"test",
			null,
			new AppenderRef[]{appenderRef},
			null,
			LOGGER_CONTEXT.getConfiguration(),
			null);
		logger.addAppender(testAppender, null, null);

		LOGGER_CONTEXT.getConfiguration().addLogger(loggerName, logger);
		LOGGER_CONTEXT.updateLoggers();
	}

	@Override
	protected void after() {
		LOGGER_CONTEXT.getConfiguration().removeLogger(loggerName);
		LOGGER_CONTEXT.updateLoggers();
		loggingEvents = null;
	}
}
