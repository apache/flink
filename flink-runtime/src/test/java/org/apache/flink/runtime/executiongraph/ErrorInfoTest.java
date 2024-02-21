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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.testutils.ClassLoaderUtils;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple test for the {@link ErrorInfo}. */
class ErrorInfoTest {

    @Test
    void testSerializationWithExceptionOutsideClassLoader() throws Exception {
        final ErrorInfo error =
                new ErrorInfo(new ExceptionWithCustomClassLoader(), System.currentTimeMillis());
        final ErrorInfo copy = CommonTestUtils.createCopySerializable(error);

        assertThat(copy.getTimestamp()).isEqualTo(error.getTimestamp());
        assertThat(copy.getExceptionAsString()).isEqualTo(error.getExceptionAsString());
        assertThat(copy.getException().getMessage()).isEqualTo(error.getException().getMessage());
    }

    // ------------------------------------------------------------------------

    private static final class ExceptionWithCustomClassLoader extends Exception {

        private static final long serialVersionUID = 42L;

        @SuppressWarnings("unused")
        private final Serializable outOfClassLoader =
                ClassLoaderUtils.createSerializableObjectFromNewClassLoader().getObject();

        public ExceptionWithCustomClassLoader() {
            super("tada");
        }
    }
}
