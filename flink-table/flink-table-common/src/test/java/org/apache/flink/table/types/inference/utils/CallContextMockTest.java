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

package org.apache.flink.table.types.inference.utils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CallContextMock}. */
class CallContextMockTest {

    @Test
    void testIsArgumentLiteralWithOutOfBoundsIndex() {
        CallContextMock mock = new CallContextMock();
        mock.argumentLiterals = Collections.singletonList(true);

        assertThat(mock.isArgumentLiteral(0)).isTrue();
        // Should safely return false instead of throwing IndexOutOfBoundsException
        assertThat(mock.isArgumentLiteral(1)).isFalse();
    }

    @Test
    void testIsArgumentLiteralWithNullList() {
        CallContextMock mock = new CallContextMock();
        mock.argumentLiterals = null;

        // Should safely return false instead of throwing NullPointerException
        assertThat(mock.isArgumentLiteral(0)).isFalse();
    }

    @Test
    void testIsArgumentNullWithOutOfBoundsIndex() {
        CallContextMock mock = new CallContextMock();
        mock.argumentNulls = Collections.singletonList(false);

        assertThat(mock.isArgumentNull(0)).isFalse();
        // Should safely return true instead of throwing IndexOutOfBoundsException
        assertThat(mock.isArgumentNull(1)).isTrue();
    }

    @Test
    void testIsArgumentNullWithNullList() {
        CallContextMock mock = new CallContextMock();
        mock.argumentNulls = null;

        // Should safely return true instead of throwing NullPointerException
        assertThat(mock.isArgumentNull(0)).isTrue();
    }

    @Test
    void testGetArgumentValueWithOutOfBoundsIndex() {
        CallContextMock mock = new CallContextMock();
        mock.argumentValues = Collections.singletonList(Optional.of("value"));

        assertThat(mock.getArgumentValue(0, String.class)).contains("value");
        // Should safely return Optional.empty() instead of throwing IndexOutOfBoundsException
        assertThat(mock.getArgumentValue(1, String.class)).isEmpty();
    }

    @Test
    void testGetArgumentValueWithNullList() {
        CallContextMock mock = new CallContextMock();
        mock.argumentValues = null;

        // Should safely return Optional.empty() instead of throwing NullPointerException
        assertThat(mock.getArgumentValue(0, String.class)).isEmpty();
    }
}
