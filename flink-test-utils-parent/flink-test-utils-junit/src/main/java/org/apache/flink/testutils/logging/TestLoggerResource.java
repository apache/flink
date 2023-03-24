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
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import org.junit.rules.ExternalResource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Utility for auditing logged messages.
 *
 * <p>Note: If a parent logger is already defined with a less specific log level, then using the
 * {@link TestLoggerResource} can increase the number of log messages received by this logger.
 *
 * <p>Example: You define a logger for org.apache.runtime with log level INFO and then use {@link
 * TestLoggerResource} to listen on org.apache.runtime.feature with log level DEBUG. In this case,
 * the log output for the parent logger will include DEBUG messages org.apache.runtime.feature.
 *
 * <p>Implementation note: Make sure to not expose log4j dependencies in the interface of this class
 * to ease updates in logging infrastructure.
 */
public class TestLoggerResource extends ExternalResource {
    private static final LoggerContext LOGGER_CONTEXT =
            (LoggerContext) LogManager.getContext(false);

    private final String loggerName;
    private final org.slf4j.event.Level level;
    @Nullable private LoggerConfig backupLoggerConfig = null;

    private ConcurrentLinkedQueue<String> loggingEvents;

    public TestLoggerResource(Class<?> clazz, org.slf4j.event.Level level) {
        this(clazz.getCanonicalName(), level);
    }

    private TestLoggerResource(String loggerName, org.slf4j.event.Level level) {
        this.loggerName = loggerName;
        this.level = level;
    }

    public List<String> getMessages() {
        return new ArrayList<>(loggingEvents);
    }

    private static String generateRandomString() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    protected void before() throws Throwable {
        loggingEvents = new ConcurrentLinkedQueue<>();

        final LoggerConfig previousLoggerConfig =
                LOGGER_CONTEXT.getConfiguration().getLoggerConfig(loggerName);

        final Level previousLevel = previousLoggerConfig.getLevel();
        final Level userDefinedLevel = Level.getLevel(level.name());

        // Set log level to least specific. This ensures that the parent still receives all log
        // lines.
        // WARN is more specific than INFO is more specific than DEBUG etc.
        final Level newLevel =
                userDefinedLevel.isMoreSpecificThan(previousLevel)
                        ? previousLevel
                        : userDefinedLevel;

        // Filter log lines according to user requirements.
        final Filter levelFilter =
                ThresholdFilter.createFilter(
                        userDefinedLevel, Filter.Result.ACCEPT, Filter.Result.DENY);

        final Appender testAppender =
                new AbstractAppender(
                        "test-appender-" + generateRandomString(),
                        levelFilter,
                        null,
                        false,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        loggingEvents.add(event.getMessage().getFormattedMessage());
                    }
                };
        testAppender.start();

        final LoggerConfig loggerConfig =
                LoggerConfig.createLogger(
                        true,
                        newLevel,
                        loggerName,
                        null,
                        new AppenderRef[] {},
                        null,
                        LOGGER_CONTEXT.getConfiguration(),
                        null);
        loggerConfig.addAppender(testAppender, null, null);

        if (previousLoggerConfig.getName().equals(loggerName)) {
            // remove the previous logger config for the duration of the test
            backupLoggerConfig = previousLoggerConfig;
            LOGGER_CONTEXT.getConfiguration().removeLogger(loggerName);

            // combine appender set
            // Note: The appender may still receive more or less messages depending on the log level
            // difference between the two logger
            for (Appender appender : previousLoggerConfig.getAppenders().values()) {
                loggerConfig.addAppender(appender, null, null);
            }
        }

        LOGGER_CONTEXT.getConfiguration().addLogger(loggerName, loggerConfig);
        LOGGER_CONTEXT.updateLoggers();
    }

    @Override
    protected void after() {
        LOGGER_CONTEXT.getConfiguration().removeLogger(loggerName);
        if (backupLoggerConfig != null) {
            LOGGER_CONTEXT.getConfiguration().addLogger(loggerName, backupLoggerConfig);
            backupLoggerConfig = null;
        }
        LOGGER_CONTEXT.updateLoggers();
        loggingEvents = null;
    }

    /** Enables the use of {@link TestLoggerResource} for try-with-resources statement. */
    public static SingleTestResource asSingleTestResource(
            String loggerName, org.slf4j.event.Level level) throws Throwable {
        return new SingleTestResource(loggerName, level);
    }

    /**
     * SingleTestResource re-uses the code in {@link TestLoggerResource} for try-with-resources
     * statement.
     */
    public static class SingleTestResource implements AutoCloseable {
        final TestLoggerResource resource;

        private SingleTestResource(String loggerName, org.slf4j.event.Level level)
                throws Throwable {
            resource = new TestLoggerResource(loggerName, level);
            resource.before();
        }

        @Override
        public void close() throws Exception {
            resource.after();
        }

        public List<String> getMessages() {
            return resource.getMessages();
        }
    }
}
