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

import org.apache.flink.annotation.Internal;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/** Class acting as a classifier for exception by failed requests to be retried by sink. */
@Internal
public class RetryValidationStrategy {
    private final Function<Throwable, Exception> throwableMapper;
    private final Predicate<Throwable> validator;
    private RetryValidationStrategy chainedStrategy;

    public RetryValidationStrategy(
            Predicate<Throwable> validator, Function<Throwable, Exception> throwableMapper) {
        this.throwableMapper = throwableMapper;
        this.validator = validator;
        this.chainedStrategy = null;
    }

    public boolean shouldRetry(Throwable err, Consumer<Exception> throwableConsumer) {
        if (validator.test(err)) {
            throwableConsumer.accept(throwableMapper.apply(err));
            return false;
        }

        if (chainedStrategy != null) {
            return chainedStrategy.shouldRetry(err, throwableConsumer);
        } else {
            return true;
        }
    }

    public static RetryValidationStrategy build(RetryValidationStrategy... strategies) {
        for (int i = 1; i < strategies.length; ++i) {
            strategies[i - 1].chainedStrategy = strategies[i];
        }
        return strategies[0];
    }
}
