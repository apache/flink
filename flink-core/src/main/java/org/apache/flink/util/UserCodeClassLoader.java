/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

/**
 * UserCodeClassLoader allows to register release hooks for a user code class loader.
 *
 * <p>These release hooks are being executed just before the user code class loader is being
 * released.
 */
public interface UserCodeClassLoader {

    /**
     * Obtains the actual class loader.
     *
     * @return actual class loader
     */
    ClassLoader asClassLoader();

    /**
     * Registers a release hook which is being executed before the user code class loader is being
     * released.
     *
     * @param releaseHookName
     * @param releaseHook releaseHook which is executed before the user code class loader is being
     *     released.
     */
    void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook);
}
