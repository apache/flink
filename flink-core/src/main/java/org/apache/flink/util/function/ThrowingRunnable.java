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

package org.apache.flink.util.function;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.ExceptionUtils;

/**
 * Similar to a {@link Runnable}, this interface is used to capture a block of code to be executed.
 * In contrast to {@code Runnable}, this interface allows throwing checked exceptions.
 */
@PublicEvolving
@FunctionalInterface
public interface ThrowingRunnable<E extends Throwable> {

    /**
     * The work method.
     *
     * @throws E Exceptions may be thrown.
     */
    void run() throws E;

    /**
     * Converts a {@link ThrowingRunnable} into a {@link Runnable} which throws all checked
     * exceptions as unchecked.
     *
     * @param throwingRunnable to convert into a {@link Runnable}
     * @return {@link Runnable} which throws all checked exceptions as unchecked.
     */
    static Runnable unchecked(ThrowingRunnable<?> throwingRunnable) {
        return () -> {
            try {
                throwingRunnable.run();
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            }
        };
    }
}
