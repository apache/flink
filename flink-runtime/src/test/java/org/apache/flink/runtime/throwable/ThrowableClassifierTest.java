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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test {@link ThrowableClassifier}. */
public class ThrowableClassifierTest extends TestLogger {

    @Test
    public void testThrowableType_NonRecoverable() {
        assertEquals(
                ThrowableType.NonRecoverableError,
                ThrowableClassifier.getThrowableType(
                        new SuppressRestartsException(new Exception(""))));
    }

    @Test
    public void testThrowableType_Recoverable() {
        assertEquals(
                ThrowableType.RecoverableError,
                ThrowableClassifier.getThrowableType(new Exception("")));
        assertEquals(
                ThrowableType.RecoverableError,
                ThrowableClassifier.getThrowableType(new TestRecoverableErrorException()));
    }

    @Test
    public void testThrowableType_EnvironmentError() {
        assertEquals(
                ThrowableType.EnvironmentError,
                ThrowableClassifier.getThrowableType(new TestEnvironmentErrorException()));
    }

    @Test
    public void testThrowableType_PartitionDataMissingError() {
        assertEquals(
                ThrowableType.PartitionDataMissingError,
                ThrowableClassifier.getThrowableType(new TestPartitionDataMissingErrorException()));
    }

    @Test
    public void testThrowableType_InheritError() {
        assertEquals(
                ThrowableType.PartitionDataMissingError,
                ThrowableClassifier.getThrowableType(
                        new TestPartitionDataMissingErrorSubException()));
    }

    @Test
    public void testFindThrowableOfThrowableType() {
        // no throwable type
        assertFalse(
                ThrowableClassifier.findThrowableOfThrowableType(
                                new Exception(), ThrowableType.RecoverableError)
                        .isPresent());

        // no recoverable throwable type
        assertFalse(
                ThrowableClassifier.findThrowableOfThrowableType(
                                new TestPartitionDataMissingErrorException(),
                                ThrowableType.RecoverableError)
                        .isPresent());

        // direct recoverable throwable
        assertTrue(
                ThrowableClassifier.findThrowableOfThrowableType(
                                new TestRecoverableErrorException(), ThrowableType.RecoverableError)
                        .isPresent());

        // nested recoverable throwable
        assertTrue(
                ThrowableClassifier.findThrowableOfThrowableType(
                                new Exception(new TestRecoverableErrorException()),
                                ThrowableType.RecoverableError)
                        .isPresent());

        // inherit recoverable throwable
        assertTrue(
                ThrowableClassifier.findThrowableOfThrowableType(
                                new TestRecoverableFailureSubException(),
                                ThrowableType.RecoverableError)
                        .isPresent());
    }

    @ThrowableAnnotation(ThrowableType.PartitionDataMissingError)
    private class TestPartitionDataMissingErrorException extends Exception {}

    @ThrowableAnnotation(ThrowableType.EnvironmentError)
    private class TestEnvironmentErrorException extends Exception {}

    @ThrowableAnnotation(ThrowableType.RecoverableError)
    private class TestRecoverableErrorException extends Exception {}

    private class TestPartitionDataMissingErrorSubException
            extends TestPartitionDataMissingErrorException {}

    private class TestRecoverableFailureSubException extends TestRecoverableErrorException {}
}
