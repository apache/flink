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

package org.apache.flink.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.slf4j.Log4jLogger;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A rule that sets the log level for specific class/package loggers for a test. Logging
 * configuration will only be extended when logging is enabled at all (so root logger is not OFF).
 */
public class LogLevelRule extends ExternalResource {
    public static final boolean LOGGING_ENABLED =
            LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).isErrorEnabled();
    private Map<String, Level> testLevels = new HashMap<>();
    private List<Runnable> resetActions = new ArrayList<>();
    private static final Map<Level, org.apache.logging.log4j.Level> SLF_TO_LOG4J = new HashMap<>();
    private LoggerContext log4jContext;

    static {
        SLF_TO_LOG4J.put(Level.ERROR, org.apache.logging.log4j.Level.ERROR);
        SLF_TO_LOG4J.put(Level.WARN, org.apache.logging.log4j.Level.WARN);
        SLF_TO_LOG4J.put(Level.INFO, org.apache.logging.log4j.Level.INFO);
        SLF_TO_LOG4J.put(Level.DEBUG, org.apache.logging.log4j.Level.DEBUG);
        SLF_TO_LOG4J.put(Level.TRACE, org.apache.logging.log4j.Level.TRACE);
    }

    @Override
    public void before() throws Exception {
        if (!LOGGING_ENABLED) {
            return;
        }

        for (Map.Entry<String, Level> levelEntry : testLevels.entrySet()) {
            final Logger logger = LoggerFactory.getLogger(levelEntry.getKey());
            if (logger instanceof Log4jLogger) {
                setLog4jLevel(levelEntry.getKey(), levelEntry.getValue());
            } else {
                throw new UnsupportedOperationException("Cannot change log level of " + logger);
            }
        }

        if (log4jContext != null) {
            log4jContext.updateLoggers();
        }
    }

    private void setLog4jLevel(String logger, Level level) {
        if (log4jContext == null) {
            log4jContext = (LoggerContext) LogManager.getContext(false);
        }
        final Configuration conf = log4jContext.getConfiguration();
        LoggerConfig loggerConfig = conf.getLoggers().get(logger);
        if (loggerConfig != null) {
            final org.apache.logging.log4j.Level oldLevel = loggerConfig.getLevel();
            loggerConfig.setLevel(SLF_TO_LOG4J.get(level));
            resetActions.add(() -> loggerConfig.setLevel(oldLevel));
        } else {
            conf.addLogger(logger, new LoggerConfig(logger, SLF_TO_LOG4J.get(level), true));
            resetActions.add(() -> conf.removeLogger(logger));
        }
    }

    @Override
    public void after() {
        resetActions.forEach(Runnable::run);

        if (log4jContext != null) {
            log4jContext.updateLoggers();
        }
    }

    public LogLevelRule set(Class<?> clazz, Level level) {
        return set(clazz.getName(), level);
    }

    public LogLevelRule set(Package logPackage, Level level) {
        return set(logPackage.getName(), level);
    }

    public LogLevelRule set(String classOrPackageName, Level level) {
        testLevels.put(classOrPackageName, level);
        return this;
    }
}
