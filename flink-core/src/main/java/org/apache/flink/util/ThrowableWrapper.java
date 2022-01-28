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

import org.apache.flink.annotation.Internal;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/** Util class acting as a classifier for exception to suppress. */
@Internal
public class ThrowableWrapper {
    private final Function<Throwable, Exception> throwableMapper;
    private final Predicate<Throwable> validator;
    private ThrowableWrapper chainedWrapper;

    public ThrowableWrapper(
            Predicate<Throwable> validator, Function<Throwable, Exception> throwableMapper) {
        this.throwableMapper = throwableMapper;
        this.validator = validator;
        this.chainedWrapper = null;
    }

    public boolean shouldSuppress(Throwable err, Consumer<Exception> throwableConsumer) {
        if (validator.test(err)) {
            throwableConsumer.accept(throwableMapper.apply(err));
            return false;
        }

        if (chainedWrapper != null) {
            return chainedWrapper.shouldSuppress(err, throwableConsumer);
        } else {
            return true;
        }
    }

    @Override
    public ThrowableWrapper clone() {
        ThrowableWrapper clonedSuppressor = new ThrowableWrapper(validator, throwableMapper);
        Optional.ofNullable(this.chainedWrapper)
                .ifPresent(wrapper -> clonedSuppressor.chainedWrapper = wrapper.clone());
        return clonedSuppressor;
    }

    public static ThrowableWrapper withRootCauseOfType(
            Class<? extends Throwable> type, Function<Throwable, Exception> mapper) {
        return new ThrowableWrapper(
                err -> ExceptionUtils.findThrowable(err, type).isPresent(), mapper);
    }

    public static ThrowableWrapper withRootCauseWithMessage(
            String message, Function<Throwable, Exception> mapper) {
        return new ThrowableWrapper(
                err -> ExceptionUtils.findThrowableWithMessage(err, message).isPresent(), mapper);
    }

    public static ThrowableWrapper build(ThrowableWrapper... classifiers) {
        for (int i = 1; i < classifiers.length; ++i) {
            classifiers[i - 1].chainedWrapper = classifiers[i];
        }
        return classifiers[0];
    }
}
