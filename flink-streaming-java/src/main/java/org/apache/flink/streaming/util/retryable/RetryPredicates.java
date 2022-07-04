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

package org.apache.flink.streaming.util.retryable;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Predicate;

/** Utility class to create concrete retry predicates. */
public class RetryPredicates {

    /** A predicate matches empty result which means an empty {@link Collection}. */
    public static final EmptyResultPredicate EMPTY_RESULT_PREDICATE = new EmptyResultPredicate();

    /** A predicate matches any exception which means a non-null{@link Throwable}. */
    public static final HasExceptionPredicate HAS_EXCEPTION_PREDICATE = new HasExceptionPredicate();

    /**
     * Creates a predicate on given exception type.
     *
     * @param exceptionClass
     * @return predicate on exception type.
     */
    public static ExceptionTypePredicate createExceptionTypePredicate(
            @Nonnull Class<? extends Throwable> exceptionClass) {
        return new ExceptionTypePredicate(exceptionClass);
    }

    private static final class EmptyResultPredicate<T>
            implements Predicate<Collection<T>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(Collection<T> ts) {
            if (null == ts || ts.isEmpty()) {
                return true;
            }
            return false;
        }
    }

    private static final class HasExceptionPredicate implements Predicate<Throwable>, Serializable {
        private static final long serialVersionUID = 1L;

        private HasExceptionPredicate() {}

        @Override
        public boolean test(Throwable throwable) {
            return null != throwable;
        }
    }

    private static final class ExceptionTypePredicate
            implements Predicate<Throwable>, Serializable {
        private static final long serialVersionUID = 1L;
        private final Class<? extends Throwable> exceptionClass;

        public ExceptionTypePredicate(@Nonnull Class<? extends Throwable> exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

        @Override
        public boolean test(@Nonnull Throwable throwable) {
            return exceptionClass.isAssignableFrom(throwable.getClass());
        }
    }
}
