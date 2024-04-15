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

package org.apache.flink.table.runtime.operators.calc.async;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Predicate;

/** Contains retry predicates used to determine if a result or error should result in a retry. */
public class RetryPredicates {

    public static final EmptyResponseResultStrategy EMPTY_RESPONSE =
            new EmptyResponseResultStrategy();
    public static final Predicate<Throwable> ANY_EXCEPTION = new AnyExceptionStrategy();

    /** Returns true if the response is null or empty. */
    public static class EmptyResponseResultStrategy
            implements Predicate<Collection<RowData>>, Serializable {
        private static final long serialVersionUID = 5065281655787318565L;

        @Override
        public boolean test(Collection<RowData> c) {
            return null == c || c.isEmpty();
        }
    }

    /** Returns true for any exception. */
    public static class AnyExceptionStrategy implements Predicate<Throwable>, Serializable {

        private static final long serialVersionUID = -46468466280154192L;

        @Override
        public boolean test(Throwable throwable) {
            return true;
        }
    }
}
