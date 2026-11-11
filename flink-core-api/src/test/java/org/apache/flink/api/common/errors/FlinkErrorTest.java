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

package org.apache.flink.api.common.errors;

import org.apache.flink.api.common.errors.FlinkError.Parameter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkError}. */
class FlinkErrorTest {

    @Test
    void testFormat() {
        assertThat(TestError.OBJECT_NOT_FOUND.format(Arrays.asList("t", TestKind.TABLE)))
                .isEqualTo("Object t of kind TABLE does not exist.");
        assertThat(TestError.OBJECT_NOT_FOUND.format(Arrays.asList("{kind}", TestKind.TABLE)))
                .isEqualTo("Object {kind} of kind TABLE does not exist.");
    }

    @Test
    void testFormatRedacted() {
        assertThat(
                        TestError.OBJECT_NOT_FOUND.formatRedacted(
                                Arrays.asList("t", TestKind.TABLE), "<redacted>"))
                .isEqualTo("Object <redacted> of kind TABLE does not exist.");
    }

    @Test
    void testInvalidArguments() {
        assertThatThrownBy(() -> TestError.OBJECT_NOT_FOUND.format(Collections.singletonList("t")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error 'OBJECT_NOT_FOUND' expects 2 arguments, but got 1.");
        assertThatThrownBy(() -> TestError.OBJECT_NOT_FOUND.format(Arrays.asList("t", "TABLE")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Error 'OBJECT_NOT_FOUND' expects an argument of type "
                                + TestKind.class.getTypeName()
                                + " for parameter 'kind', but got java.lang.String.");
    }

    @Test
    void testInvalidParameters() {
        assertThatThrownBy(() -> Parameter.of("object name", String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must consist of letters and digits");
        assertThatThrownBy(() -> Parameter.of("value", Object.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Parameter 'value' must have a serializable type, but has java.lang.Object.");
        assertThatThrownBy(() -> Parameter.of("columns", String[].class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Parameter 'columns' must not have an array or exception type, "
                                + "but has java.lang.String[].");
    }

    private enum TestKind {
        TABLE
    }

    private enum TestError implements FlinkError {
        OBJECT_NOT_FOUND(
                "Object {objectName} of kind {kind} does not exist.",
                Parameter.sensitive("objectName", String.class),
                Parameter.of("kind", TestKind.class));

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
