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

package org.apache.flink.runtime.asyncprocessing.declare;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.function.BiFunctionWithException;

import java.util.function.BiFunction;

/** A named version of {@link BiFunction}. */
@Experimental
public class NamedBiFunction<T, U, V> extends NamedCallback
        implements BiFunctionWithException<T, U, V, Exception> {

    BiFunctionWithException<T, U, V, ? extends Exception> function;

    public NamedBiFunction(
            String name, BiFunctionWithException<T, U, V, ? extends Exception> function) {
        super(name);
        this.function = function;
    }

    @Override
    public V apply(T t, U u) throws Exception {
        return function.apply(t, u);
    }
}
