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

package org.apache.flink.runtime.throwable;

import org.apache.flink.runtime.execution.SuppressRestartsException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link ThrowableClassifier}. */
class ThrowableClassifierTest {

    @Test
    void testThrowableType_NonRecoverable() {
        assertThat(
                        ThrowableClassifier.getThrowableType(
                                new SuppressRestartsException(new Exception(""))))
                .isEqualTo(ThrowableType.NonRecoverableError);
    }

    @Test
    void testThrowableType_Recoverable() {
        assertThat(ThrowableClassifier.getThrowableType(new Exception("")))
                .isEqualTo(ThrowableType.RecoverableError);
        assertThat(ThrowableClassifier.getThrowableType(new TestRecoverableErrorException()))
                .isEqualTo(ThrowableType.RecoverableError);
    }

    @Test
    void testThrowableType_EnvironmentError() {
        assertThat(ThrowableClassifier.getThrowableType(new TestEnvironmentErrorException()))
                .isEqualTo(ThrowableType.EnvironmentError);
    }

    @Test
    void testThrowableType_PartitionDataMissingError() {
        assertThat(
                        ThrowableClassifier.getThrowableType(
                                new TestPartitionDataMissingErrorException()))
                .isEqualTo(ThrowableType.PartitionDataMissingError);
    }

    @Test
    void testThrowableType_InheritError() {
        assertThat(
                        ThrowableClassifier.getThrowableType(
                                new TestPartitionDataMissingErrorSubException()))
                .isEqualTo(ThrowableType.PartitionDataMissingError);
    }

    @Test
    void testFindThrowableOfThrowableType() {
        // no throwable type
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                new Exception(), ThrowableType.RecoverableError))
                .isNotPresent();

        // no recoverable throwable type
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                new TestPartitionDataMissingErrorException(),
                                ThrowableType.RecoverableError))
                .isNotPresent();

        // direct recoverable throwable
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                new TestRecoverableErrorException(),
                                ThrowableType.RecoverableError))
                .isPresent();

        // nested recoverable throwable
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                new Exception(new TestRecoverableErrorException()),
                                ThrowableType.RecoverableError))
                .isPresent();

        // inherit recoverable throwable
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                new TestRecoverableFailureSubException(),
                                ThrowableType.RecoverableError))
                .isPresent();
    }

    @ThrowableAnnotation(ThrowableType.PartitionDataMissingError)
    private static class TestPartitionDataMissingErrorException extends Exception {}

    @ThrowableAnnotation(ThrowableType.EnvironmentError)
    private static class TestEnvironmentErrorException extends Exception {}

    @ThrowableAnnotation(ThrowableType.RecoverableError)
    private static class TestRecoverableErrorException extends Exception {}

    private static class TestPartitionDataMissingErrorSubException
            extends TestPartitionDataMissingErrorException {}

    private static class TestRecoverableFailureSubException extends TestRecoverableErrorException {}
}
