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

package org.apache.flink.table.factories;

import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/** This class contains utilities to deal with {@link ServiceLoader}. */
class ServiceLoaderUtil {

    /**
     * Tries to load all This method behaves similarly to {@link ServiceLoader#load(Class,
     * ClassLoader)}, but it returns a list with the results of the iteration, wrapping the
     * iteration failures such as {@link NoClassDefFoundError}.
     */
    static <S extends Throwable, T> List<T> load(
            Class<T> clazz,
            ClassLoader classLoader,
            ThrowingConsumer<Throwable, S> errorHandlingCallback)
            throws S {
        List<T> loadResults = new ArrayList<>();

        Iterator<T> serviceLoaderIterator = ServiceLoader.load(clazz, classLoader).iterator();

        while (true) {
            try {
                // error handling should also be applied to the hasNext() call because service
                // loading might cause problems here as well
                if (!serviceLoaderIterator.hasNext()) {
                    break;
                }

                T next = serviceLoaderIterator.next();
                loadResults.add(next);
            } catch (Throwable t) {
                errorHandlingCallback.accept(t);
            }
        }

        return loadResults;
    }
}
