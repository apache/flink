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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests the RetryValidationStrategy of the Async Sink Writer. */
public class RetryValidationStrategyTest {

    private static Integer nullReference;

    private static final RetryValidationStrategy ARITHMETIC_EXCEPTION_STRATEGY =
            new RetryValidationStrategy(
                    err -> ExceptionUtils.findThrowable(err, ArithmeticException.class).isPresent(),
                    err ->
                            new RuntimeException(
                                    "Buffer manipulation calculations resulted in a calculation exception",
                                    err));

    private static final RetryValidationStrategy NULL_POINTER_EXCEPTION_STRATEGY =
            new RetryValidationStrategy(
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
        try {
            int failedCalculation = 100 / 0;
        } catch (Exception e) {
            ARITHMETIC_EXCEPTION_STRATEGY.shouldRetry(e, caughtExceptionReference::set);
        }
        assertThatCaughtExceptionIsWrappedArithmeticDivByZeroException(
                caughtExceptionReference.get());
    }

    @Test
    public void noExceptionIsThrownIfTheExceptionDoesNotMatchTheOneExpected() {
        AtomicReference<Exception> caughtException = new AtomicReference<>();
        try {
            System.out.print(nullReference.toString());
        } catch (Exception e) {
            ARITHMETIC_EXCEPTION_STRATEGY.shouldRetry(e, caughtException::set);
        }
        assertThat(caughtException.get()).isNull();
    }

    @Test
    public void chainedRetryStrategiesAcceptExceptionsOnTheFirstItemOfChain() {
        RetryValidationStrategy retryValidationStrategy =
                RetryValidationStrategy.build(
                        ARITHMETIC_EXCEPTION_STRATEGY, NULL_POINTER_EXCEPTION_STRATEGY);
        AtomicReference<Exception> caughtExceptionReference = new AtomicReference<>();
        try {
            int failedCalculation = 100 / 0;
        } catch (Exception e) {
            retryValidationStrategy.shouldRetry(e, caughtExceptionReference::set);
        }
        assertThatCaughtExceptionIsWrappedArithmeticDivByZeroException(
                caughtExceptionReference.get());
    }

    @Test
    public void chainedRetryStrategiesAcceptExceptionsOnTheLastItemOfChain() {
        RetryValidationStrategy retryValidationStrategy =
                RetryValidationStrategy.build(
                        ARITHMETIC_EXCEPTION_STRATEGY, NULL_POINTER_EXCEPTION_STRATEGY);
        AtomicReference<Exception> caughtException = new AtomicReference<>();

        try {
            System.out.print(nullReference.toString());
        } catch (Exception e) {
            retryValidationStrategy.shouldRetry(e, caughtException::set);
        }

        assertThat(caughtException.get())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Buffer manipulation calculations resulted in a reference exception")
                .getCause()
                .isInstanceOf(NullPointerException.class);
    }

    private void assertThatCaughtExceptionIsWrappedArithmeticDivByZeroException(
            Exception caughtException) {
        assertThat(caughtException)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Buffer manipulation calculations resulted in a calculation exception")
                .getCause()
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("/ by zero");
    }
}
