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

package org.apache.flink.testutils.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TestLoggerResourceTest} ensures that the use of {@link TestLoggerResource} combined with
 * other loggers (including multiple instances of itself) does not lead to unexpected behavior.
 */
class TestLoggerResourceTest {
    private static final String parentLoggerName =
            TestLoggerResourceTest.class.getName() + ".parent";
    private static final String childLoggerName = parentLoggerName + ".child";
    private static final Logger parentLogger = LogManager.getLogger(parentLoggerName);
    private static final Logger childLogger = LogManager.getLogger(childLoggerName);

    @Test
    void loggerWithoutChild() throws Throwable {
        try (TestLoggerResource.SingleTestResource parentResource =
                TestLoggerResource.asSingleTestResource(parentLoggerName, Level.INFO)) {
            parentLogger.info("parent-info");
            parentLogger.debug("parent-debug");
            final List<String> msgs = parentResource.getMessages();
            assertThat(msgs).containsExactly("parent-info");
        }
    }

    @Test
    void loggerIsAlreadyDefinedOriginalInfoNewDebug() throws Throwable {
        try (TestLoggerResource.SingleTestResource outerResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.INFO);
                TestLoggerResource.SingleTestResource innerResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.DEBUG)) {
            parentLogger.info("parent-info");
            parentLogger.debug("parent-debug");
            final List<String> msgsInner = innerResource.getMessages();
            assertThat(msgsInner).containsExactly("parent-info", "parent-debug");
            final List<String> msgsOuter = outerResource.getMessages();
            assertThat(msgsOuter).containsExactly("parent-info");
        }
    }

    @Test
    void loggerIsAlreadyDefinedOriginalDebugNewInfo() throws Throwable {
        try (TestLoggerResource.SingleTestResource outerResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.DEBUG);
                TestLoggerResource.SingleTestResource innerResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.INFO)) {
            parentLogger.info("parent-info");
            parentLogger.debug("parent-debug");
            final List<String> msgsInner = innerResource.getMessages();
            assertThat(msgsInner).containsExactly("parent-info");
            final List<String> msgsOuter = outerResource.getMessages();
            assertThat(msgsOuter).containsExactly("parent-info", "parent-debug");
        }
    }

    @Test
    void parentInfoLevelChildInfoLevel() throws Throwable {
        try (TestLoggerResource.SingleTestResource parentResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.INFO);
                TestLoggerResource.SingleTestResource childResource =
                        TestLoggerResource.asSingleTestResource(childLoggerName, Level.INFO)) {
            parentLogger.info("parent-info");
            parentLogger.debug("parent-debug");
            childLogger.info("child-info");
            childLogger.debug("child-debug");
            final List<String> parentMsgs = parentResource.getMessages();
            assertThat(parentMsgs).containsExactly("parent-info", "child-info");
            final List<String> childMsgs = childResource.getMessages();
            assertThat(childMsgs).containsExactly("child-info");
        }
    }

    @Test
    void parentDebugLevelChildInfoLevel() throws Throwable {
        try (TestLoggerResource.SingleTestResource parentResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.DEBUG);
                TestLoggerResource.SingleTestResource childResource =
                        TestLoggerResource.asSingleTestResource(childLoggerName, Level.INFO)) {
            parentLogger.info("parent-info");
            parentLogger.debug("parent-debug");
            childLogger.info("child-info");
            childLogger.debug("child-debug");
            final List<String> parentMsgs = parentResource.getMessages();
            assertThat(parentMsgs)
                    .containsExactly("parent-info", "parent-debug", "child-info", "child-debug");
            final List<String> childMsgs = childResource.getMessages();
            assertThat(childMsgs).containsExactly("child-info");
        }
    }

    @Test
    void parentInfoLevelChildDebugLevel() throws Throwable {
        // Note: Here we receive debug messages for the parent logger even though its own log level
        // is set to INFO. To change this, we would need to add a filter to the appender.
        try (TestLoggerResource.SingleTestResource parentResource =
                        TestLoggerResource.asSingleTestResource(parentLoggerName, Level.INFO);
                TestLoggerResource.SingleTestResource childResource =
                        TestLoggerResource.asSingleTestResource(childLoggerName, Level.DEBUG)) {
            parentLogger.info("parent-info");
            parentLogger.debug("parent-debug");
            childLogger.info("child-info");
            childLogger.debug("child-debug");
            final List<String> parentMsgs = parentResource.getMessages();
            assertThat(parentMsgs).containsExactly("parent-info", "child-info");
            final List<String> childMsgs = childResource.getMessages();
            assertThat(childMsgs).containsExactly("child-info", "child-debug");
        }
    }
}
