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

import org.apache.flink.streaming.api.functions.async.ResultFuture;

/**
 * A wrapper for {@link GeneratedResultFuture} which wraps a class instead of generated code in it.
 * It is only used for easy testing.
 */
public class GeneratedResultFutureWrapper<T extends ResultFuture<?>>
        extends GeneratedResultFuture<T> {

    private static final long serialVersionUID = 3964204655565783705L;
    private final Class<T> clazz;

    public GeneratedResultFutureWrapper(T resultFuture) {
        super(resultFuture.getClass().getSimpleName(), "N/A", new Object[0]);
        //noinspection unchecked
        this.clazz = (Class<T>) resultFuture.getClass();
    }

    @Override
    public T newInstance(ClassLoader classLoader) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not instantiate class " + clazz.getCanonicalName(), e);
        }
    }

    @Override
    public Class<T> compile(ClassLoader classLoader) {
        return clazz;
    }
}
