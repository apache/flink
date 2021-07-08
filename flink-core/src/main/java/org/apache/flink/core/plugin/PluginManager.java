/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import java.util.Iterator;

/**
 * PluginManager is responsible for managing cluster plugins which are loaded using separate class
 * loaders so that their dependencies don't interfere with Flink's dependencies.
 */
public interface PluginManager {

    /**
     * Returns in iterator over all available implementations of the given service interface (SPI)
     * in all the plugins known to this plugin manager instance.
     *
     * @param service the service interface (SPI) for which implementations are requested.
     * @param <P> Type of the requested plugin service.
     * @return Iterator over all implementations of the given service that could be loaded from all
     *     known plugins.
     */
    <P> Iterator<P> load(Class<P> service);
}
