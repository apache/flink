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

import org.apache.flink.api.common.errors.ErrorAttribute;
import org.apache.flink.api.common.errors.ErrorCategory;
import org.apache.flink.api.common.errors.FlinkError;
import org.apache.flink.api.common.errors.FlinkThrowable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link FlinkThrowable} in {@link FlinkRuntimeException} and {@link FlinkException}. */
class FlinkThrowableTest {

    @Test
    void testExceptionOfError() {
        final IOException cause = new IOException("cause");
        final FlinkRuntimeException e =
                new FlinkRuntimeException(TestError.OBJECT_NOT_FOUND, cause, "t", "db");

        assertThat(e).hasMessage("Object t does not exist in db.").hasCause(cause);
        assertThat(e.getError()).contains(TestError.OBJECT_NOT_FOUND);
        assertThat(e.getErrorArgs())
                .containsExactly(entry("objectName", "t"), entry("location", "db"));
        assertThat(e.<String>getErrorArg("location")).isEqualTo("db");
        assertThat(e.getRedactedMessage()).isEqualTo("Object <redacted> does not exist in db.");
        assertThat(e.is(TestError.OBJECT_NOT_FOUND)).isTrue();
        assertThat(e.is(TestError.OBJECT_ALREADY_EXISTS)).isFalse();
    }

    @Test
    void testExceptionWithFreeTextMessage() {
        final FlinkException e = new FlinkException("Object t does not exist.");

        assertThat(e.getError()).isEmpty();
        assertThat(e.getErrorArgs()).isEmpty();
        assertThat(e.getRedactedMessage()).isEqualTo("<redacted>");
        assertThat(e.is(TestError.OBJECT_NOT_FOUND)).isFalse();
        assertThatThrownBy(() -> e.getErrorArg("objectName"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The exception has no argument for parameter 'objectName'.");
    }

    @Test
    void testInvalidArgumentsKeepTheCause() {
        final IOException cause = new IOException("cause");

        assertThatThrownBy(() -> new FlinkException(TestError.OBJECT_NOT_FOUND, cause, "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error 'OBJECT_NOT_FOUND' expects 2 arguments, but got 1.")
                .hasCause(cause);
    }

    @Test
    void testFindCause() throws FlinkException {
        final FlinkException error = new FlinkException(TestError.OBJECT_ALREADY_EXISTS, "t");
        final RuntimeException wrapper = new RuntimeException("wrapper", error);

        assertThat(FlinkThrowable.get(error)).containsSame(error);
        assertThat(FlinkThrowable.get(wrapper)).isEmpty();
        assertThat(FlinkThrowable.get(error, TestError.OBJECT_ALREADY_EXISTS)).containsSame(error);
        assertThat(FlinkThrowable.get(error, TestError.OBJECT_NOT_FOUND)).isEmpty();
        assertThat(FlinkThrowable.getOrThrow(error, TestError.OBJECT_ALREADY_EXISTS))
                .isSameAs(error);
        assertThatThrownBy(
                        () -> FlinkThrowable.getOrThrow(wrapper, TestError.OBJECT_ALREADY_EXISTS))
                .isSameAs(wrapper);

        assertThat(FlinkThrowable.findCause(wrapper, TestError.OBJECT_ALREADY_EXISTS))
                .containsSame(error);
        assertThat(FlinkThrowable.findCause(wrapper, TestError.OBJECT_NOT_FOUND)).isEmpty();
        assertThat(FlinkThrowable.findCauseOrThrow(wrapper, TestError.OBJECT_ALREADY_EXISTS))
                .isSameAs(error);
        assertThatThrownBy(
                        () -> FlinkThrowable.findCauseOrThrow(wrapper, TestError.OBJECT_NOT_FOUND))
                .isSameAs(wrapper);

        final FlinkRuntimeException outer =
                new FlinkRuntimeException("Could not create t.", wrapper);
        assertThat(outer.findCause(TestError.OBJECT_ALREADY_EXISTS)).containsSame(error);
        assertThat(outer.containsCause(TestError.OBJECT_NOT_FOUND)).isFalse();
    }

    @Test
    void testSerialization() throws Exception {
        final FlinkRuntimeException e =
                new FlinkRuntimeException(TestError.OBJECT_NOT_FOUND, "t", "db");

        final FlinkRuntimeException copy = InstantiationUtil.clone(e);

        assertThat(copy).hasMessage("Object t does not exist in db.");
        assertThat(copy.is(TestError.OBJECT_NOT_FOUND)).isTrue();
        assertThat(copy.getErrorArgs())
                .containsExactly(entry("objectName", "t"), entry("location", "db"));
    }

    private enum TestError implements FlinkError {
        OBJECT_NOT_FOUND(
                "Object {objectName} does not exist in {location}.",
                Parameter.sensitive("objectName", String.class),
                Parameter.of("location", String.class)),

        OBJECT_ALREADY_EXISTS(
                "Object {objectName} already exists.",
                Parameter.sensitive("objectName", String.class));

        private final String messageTemplate;
        private final List<Parameter> parameters;

        TestError(String messageTemplate, Parameter... parameters) {
            this.messageTemplate = messageTemplate;
            this.parameters = Arrays.asList(parameters);
        }

        @Override
        public ErrorCategory getCategory() {
            return ErrorCategory.USER;
        }

        @Override
        public Map<ErrorAttribute, Object> getAttributes() {
            return Collections.emptyMap();
        }

        @Override
        public String getMessageTemplate() {
            return messageTemplate;
        }

        @Override
        public List<Parameter> getParameters() {
            return parameters;
        }
    }
}
