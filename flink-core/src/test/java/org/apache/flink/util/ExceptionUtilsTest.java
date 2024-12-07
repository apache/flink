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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the utility methods in {@link ExceptionUtils}. */
class ExceptionUtilsTest {

    @Test
    void testStringifyNullException() {
        assertThat(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)
                .isEqualTo(ExceptionUtils.stringifyException(null));
    }

    @Test
    void testJvmFatalError() {
        // not all errors are fatal
        assertThat(ExceptionUtils.isJvmFatalError(new Error())).isFalse();

        // linkage errors are not fatal
        assertThat(ExceptionUtils.isJvmFatalError(new LinkageError())).isFalse();

        // some errors are fatal
        assertThat(ExceptionUtils.isJvmFatalError(new InternalError())).isTrue();
        assertThat(ExceptionUtils.isJvmFatalError(new UnknownError())).isTrue();
    }

    @Test
    void testRethrowFatalError() {
        // fatal error is rethrown
        assertThatThrownBy(() -> ExceptionUtils.rethrowIfFatalError(new InternalError()))
                .isInstanceOf(InternalError.class);

        // non-fatal error is not rethrown
        ExceptionUtils.rethrowIfFatalError(new NoClassDefFoundError());
    }

    @Test
    void testFindThrowableByType() {
        assertThat(
                        ExceptionUtils.findThrowable(
                                new RuntimeException(new IllegalStateException()),
                                IllegalStateException.class))
                .isPresent();
    }

    @Test
    void testExceptionStripping() {
        final FlinkException expectedException = new FlinkException("test exception");
        final Throwable strippedException =
                ExceptionUtils.stripException(
                        new RuntimeException(new RuntimeException(expectedException)),
                        RuntimeException.class);

        assertThat(strippedException).isEqualTo(expectedException);
    }

    @Test
    void testInvalidExceptionStripping() {
        final FlinkException expectedException =
                new FlinkException(new RuntimeException(new FlinkException("inner exception")));
        final Throwable strippedException =
                ExceptionUtils.stripException(expectedException, RuntimeException.class);

        assertThat(strippedException).isEqualTo(expectedException);
    }

    @Test
    void testTryEnrichTaskExecutorErrorCanHandleNullValueWithoutCausingException() {
        ExceptionUtils.tryEnrichOutOfMemoryError(null, "", "", "");
    }

    @Test
    void testUpdateDetailMessageOfBasicThrowable() {
        Throwable rootThrowable = new OutOfMemoryError("old message");
        ExceptionUtils.updateDetailMessage(rootThrowable, t -> "new message");

        assertThat(rootThrowable.getMessage()).isEqualTo("new message");
    }

    @Test
    void testUpdateDetailMessageOfRelevantThrowableAsCause() {
        Throwable oomCause =
                new IllegalArgumentException("another message deep down in the cause tree");

        Throwable oom = new OutOfMemoryError("old message").initCause(oomCause);
        oom.setStackTrace(
                new StackTraceElement[] {new StackTraceElement("class", "method", "file", 1)});
        oom.addSuppressed(new NullPointerException());

        Throwable rootThrowable = new IllegalStateException("another message", oom);
        ExceptionUtils.updateDetailMessage(
                rootThrowable,
                t -> t.getClass().equals(OutOfMemoryError.class) ? "new message" : null);

        assertThat(rootThrowable.getCause()).isSameAs(oom);
        assertThat(rootThrowable.getCause().getMessage()).isEqualTo("new message");
        assertThat(rootThrowable.getCause().getStackTrace()).isEqualTo(oom.getStackTrace());
        assertThat(rootThrowable.getCause().getSuppressed()).isEqualTo(oom.getSuppressed());

        assertThat(rootThrowable.getCause().getCause()).isSameAs(oomCause);
    }

    @Test
    void testUpdateDetailMessageWithoutRelevantThrowable() {
        Throwable originalThrowable =
                new IllegalStateException(
                        "root message", new IllegalArgumentException("cause message"));
        ExceptionUtils.updateDetailMessage(originalThrowable, t -> null);

        assertThat(originalThrowable.getMessage()).isEqualTo("root message");
        assertThat(originalThrowable.getCause().getMessage()).isEqualTo("cause message");
    }

    @Test
    void testUpdateDetailMessageOfNullWithoutException() {
        ExceptionUtils.updateDetailMessage(null, t -> "new message");
    }

    @Test
    void testUpdateDetailMessageWithMissingPredicate() {
        Throwable root = new Exception("old message");
        ExceptionUtils.updateDetailMessage(root, null);

        assertThat(root.getMessage()).isEqualTo("old message");
    }

    @Test
    void testIsMetaspaceOutOfMemoryErrorCanHandleNullValue() {
        assertThat(ExceptionUtils.isMetaspaceOutOfMemoryError(null)).isFalse();
    }

    @Test
    void testIsDirectOutOfMemoryErrorCanHandleNullValue() {
        assertThat(ExceptionUtils.isDirectOutOfMemoryError(null)).isFalse();
    }
}
