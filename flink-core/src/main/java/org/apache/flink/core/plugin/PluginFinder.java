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

import java.io.IOException;
import java.util.Collection;

/**
 * Implementations of this interface provide mechanisms to locate plugins and create corresponding
 * {@link PluginDescriptor} objects. The result can then be used to initialize a {@link
 * PluginLoader}.
 */
public interface PluginFinder {

    /**
     * Find plugins and return a corresponding collection of {@link PluginDescriptor} instances.
     *
     * @return a collection of {@link PluginDescriptor} instances for all found plugins.
     * @throws IOException thrown if a problem occurs during plugin search.
     */
    Collection<PluginDescriptor> findPlugins() throws IOException;
}
