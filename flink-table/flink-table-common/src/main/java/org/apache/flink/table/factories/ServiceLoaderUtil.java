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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

/** This class contains utilities to deal with {@link ServiceLoader}. */
class ServiceLoaderUtil {

    /**
     * This method behaves similarly to {@link ServiceLoader#load(Class, ClassLoader)}, but it
     * returns a list with the results of the iteration, wrapping the iteration failures such as
     * {@link NoClassDefFoundError}.
     */
    static <T> List<LoadResult<T>> load(Class<T> clazz, ClassLoader classLoader) {
        List<LoadResult<T>> loadResults = new ArrayList<>();

        Iterator<T> serviceLoaderIterator = ServiceLoader.load(clazz, classLoader).iterator();

        while (true) {
            try {
                T next = serviceLoaderIterator.next();
                loadResults.add(new LoadResult<>(next));
            } catch (NoSuchElementException e) {
                break;
            } catch (Throwable t) {
                loadResults.add(new LoadResult<>(t));
            }
        }

        return loadResults;
    }

    static class LoadResult<T> {
        private final T service;
        private final Throwable error;

        private LoadResult(T service, Throwable error) {
            this.service = service;
            this.error = error;
        }

        private LoadResult(T service) {
            this(service, null);
        }

        private LoadResult(Throwable error) {
            this(null, error);
        }

        public boolean hasFailed() {
            return error != null;
        }

        public Throwable getError() {
            return error;
        }

        public T getService() {
            return service;
        }
    }
}
