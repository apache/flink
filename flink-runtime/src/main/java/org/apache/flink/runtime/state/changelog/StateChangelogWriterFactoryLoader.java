/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.plugin.PluginManager;

import java.util.Iterator;
import java.util.ServiceLoader;

import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterators.concat;

/** A thin wrapper around {@link PluginManager} to load {@link StateChangelogWriterFactory}. */
@Internal
public class StateChangelogWriterFactoryLoader {
    private final PluginManager pluginManager;

    public StateChangelogWriterFactoryLoader(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    @SuppressWarnings({"rawtypes"})
    public Iterator<StateChangelogWriterFactory> load() {
        return concat(
                pluginManager.load(StateChangelogWriterFactory.class),
                ServiceLoader.load(StateChangelogWriterFactory.class).iterator());
    }
}
