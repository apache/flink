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

package org.apache.flink.api.common.typeutils;

import java.io.Closeable;
import java.io.IOException;

/** Utility class to temporarily use a different classloader as the thread context classloader. */
public class ThreadContextClassLoader implements Closeable {

    private final ClassLoader originalThreadContextClassLoader;

    public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader) {
        this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
    }

    @Override
    public void close() throws IOException {
        Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
    }
}
