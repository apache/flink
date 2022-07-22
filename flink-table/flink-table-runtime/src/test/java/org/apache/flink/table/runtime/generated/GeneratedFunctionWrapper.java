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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.Function;

import java.util.function.Consumer;

/**
 * A wrapper for {@link GeneratedFunction} which wraps a class instead of generated code in it. It
 * is only used for easy testing.
 */
public class GeneratedFunctionWrapper<F extends Function> extends GeneratedFunction<F> {

    private static final long serialVersionUID = 5441271500290946721L;
    private final Class<F> clazz;
    private final Consumer<F> newInstanceConsumer;

    public GeneratedFunctionWrapper(F function) {
        this(function, (ignored) -> {});
    }

    public GeneratedFunctionWrapper(F function, Consumer<F> newInstanceConsumer) {
        super(function.getClass().getSimpleName(), "", new Object[0]);
        //noinspection unchecked
        this.clazz = (Class<F>) function.getClass();
        this.newInstanceConsumer = newInstanceConsumer;
    }

    @Override
    public F newInstance(ClassLoader classLoader) {
        try {
            F function = clazz.newInstance();
            newInstanceConsumer.accept(function);
            return function;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not instantiate class " + clazz.getCanonicalName(), e);
        }
    }

    @Override
    public Class<F> compile(ClassLoader classLoader) {
        return clazz;
    }
}
