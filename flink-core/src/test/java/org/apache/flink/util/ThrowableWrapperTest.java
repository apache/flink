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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests the ThrowableWrapper of the Async Sink Writer. */
public class ThrowableWrapperTest {

    private static Integer nullReference;

    private static final ThrowableWrapper ARITHMETIC_EXCEPTION_STRATEGY =
            new ThrowableWrapper(
                    err -> ExceptionUtils.findThrowable(err, ArithmeticException.class).isPresent(),
                    err ->
                            new RuntimeException(
                                    "Buffer manipulation calculations resulted in a calculation exception",
                                    err));

    private static final ThrowableWrapper NULL_POINTER_EXCEPTION_STRATEGY =
            new ThrowableWrapper(
                    err ->
                            ExceptionUtils.findThrowable(err, NullPointerException.class)
                                    .isPresent(),
                    err ->
                            new RuntimeException(
                                    "Buffer manipulation calculations resulted in a reference exception",
                                    err));

    @Test
    public void exceptionsAreWrappedInTheContainingExceptionWhenAMatchIsFound() {
        AtomicReference<Exception> caughtExceptionReference = new AtomicReference<>();

        ARITHMETIC_EXCEPTION_STRATEGY.shouldSuppress(
                new ArithmeticException("Base arithmetic exception"),
                caughtExceptionReference::set);

        assertThatCaughtExceptionIsWrappedArithmeticDivByZeroException(
                caughtExceptionReference.get());
    }

    @Test
    public void noExceptionIsThrownIfTheExceptionDoesNotMatchTheOneExpected() {
        AtomicReference<Exception> caughtException = new AtomicReference<>();
        try {
            System.out.print(nullReference.toString());
        } catch (Exception e) {
            ARITHMETIC_EXCEPTION_STRATEGY.shouldSuppress(e, caughtException::set);
        }
        assertThat(caughtException.get()).isNull();
    }

    @Test
    public void chainedRetryStrategiesAcceptExceptionsOnTheFirstItemOfChain() {
        ThrowableWrapper throwableWrapper =
                ThrowableWrapper.build(
                        ARITHMETIC_EXCEPTION_STRATEGY, NULL_POINTER_EXCEPTION_STRATEGY);
        AtomicReference<Exception> caughtExceptionReference = new AtomicReference<>();

        throwableWrapper.shouldSuppress(
                new ArithmeticException("Base arithmetic exception"),
                caughtExceptionReference::set);

        assertThatCaughtExceptionIsWrappedArithmeticDivByZeroException(
                caughtExceptionReference.get());
    }

    @Test
    public void chainedRetryStrategiesAcceptExceptionsOnTheLastItemOfChain() {
        ThrowableWrapper throwableWrapper =
                ThrowableWrapper.build(
                        ARITHMETIC_EXCEPTION_STRATEGY, NULL_POINTER_EXCEPTION_STRATEGY);
        AtomicReference<Exception> caughtException = new AtomicReference<>();

        throwableWrapper.shouldSuppress(
                new NullPointerException("Base NullPointerException"), caughtException::set);

        assertThat(caughtException.get())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Buffer manipulation calculations resulted in a reference exception")
                .getCause()
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Base NullPointerException");
    }

    private void assertThatCaughtExceptionIsWrappedArithmeticDivByZeroException(
            Exception caughtException) {
        assertThat(caughtException)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Buffer manipulation calculations resulted in a calculation exception")
                .getCause()
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Base arithmetic exception");
    }
}
