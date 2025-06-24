/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * {@link CoordinatorStore} can be used for sharing some information among {@link
 * OperatorCoordinator} instances. Motivating example is/was combining/aggregating latest watermark
 * emitted by different sources in order to do the watermark alignment.
 *
 * <p>Implementations of this interface must ensure that all operations are atomic.
 */
@ThreadSafe
@Internal
public interface CoordinatorStore {
    boolean containsKey(Object key);

    Object get(Object key);

    Object putIfAbsent(Object key, Object value);

    Object computeIfPresent(Object key, BiFunction<Object, Object, Object> remappingFunction);

    Object compute(Object key, BiFunction<Object, Object, Object> mappingFunction);

    <R> R apply(Object key, Function<Object, R> consumer);
}
