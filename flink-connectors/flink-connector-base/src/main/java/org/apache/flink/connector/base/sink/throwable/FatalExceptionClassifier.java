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

package org.apache.flink.connector.base.sink.throwable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/** Classifier class for retryable exceptions on request submission failure. */
@Internal
public class FatalExceptionClassifier {
    private final Function<Throwable, Exception> throwableMapper;
    private final Predicate<Throwable> validator;
    private FatalExceptionClassifier chainedClassifier;

    public FatalExceptionClassifier(
            Predicate<Throwable> validator, Function<Throwable, Exception> throwableMapper) {
        this.throwableMapper = throwableMapper;
        this.validator = validator;
        this.chainedClassifier = null;
    }

    public boolean isFatal(Throwable err, Consumer<Exception> throwableConsumer) {
        if (validator.test(err)) {
            throwableConsumer.accept(throwableMapper.apply(err));
            return false;
        }

        if (chainedClassifier != null) {
            return chainedClassifier.isFatal(err, throwableConsumer);
        } else {
            return true;
        }
    }

    public static FatalExceptionClassifier withRootCauseOfType(
            Class<? extends Throwable> type, Function<Throwable, Exception> mapper) {
        return new FatalExceptionClassifier(
                err -> ExceptionUtils.findThrowable(err, type).isPresent(), mapper);
    }

    public static FatalExceptionClassifier createChain(FatalExceptionClassifier... classifiers) {
        Set<FatalExceptionClassifier> importedClassifiers = new HashSet<>();

        if (classifiers.length == 0) {
            throw new IllegalArgumentException("Cannot create empty classifier chain.");
        }

        FatalExceptionClassifier tailClassifier = classifiers[0];
        importedClassifiers.add(tailClassifier);

        for (int i = 1; i < classifiers.length; ++i) {
            if (importedClassifiers.contains(classifiers[i])) {
                throw new IllegalArgumentException(
                        "Wrong classifier chain; Circular chain of classifiers detected.");
            }

            tailClassifier.chainedClassifier = classifiers[i];
            tailClassifier = classifiers[i];
            importedClassifiers.add(tailClassifier);
        }

        return classifiers[0];
    }
}
