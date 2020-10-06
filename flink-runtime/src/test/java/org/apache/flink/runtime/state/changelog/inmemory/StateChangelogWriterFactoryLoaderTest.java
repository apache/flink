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

package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.state.changelog.StateChangelogWriterFactory;
import org.apache.flink.runtime.state.changelog.StateChangelogWriterFactoryLoader;

import org.junit.Test;

import java.util.Iterator;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.apache.flink.shaded.curator4.com.google.common.collect.ImmutableList.copyOf;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertTrue;

public class StateChangelogWriterFactoryLoaderTest {

    @Test
    public void testLoadSpiImplementation() {
        assertTrue(
                new StateChangelogWriterFactoryLoader(getPluginManager(emptyIterator()))
                        .load()
                        .hasNext());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testLoadPluginImplementation() {
        StateChangelogWriterFactory<?> impl = new InMemoryStateChangelogWriterFactory();
        PluginManager pluginManager = getPluginManager(singletonList(impl).iterator());
        Iterator<StateChangelogWriterFactory> loaded =
                new StateChangelogWriterFactoryLoader(pluginManager).load();
        assertTrue(copyOf(loaded).contains(impl));
    }

    private PluginManager getPluginManager(
            Iterator<? extends StateChangelogWriterFactory<?>> iterator) {
        return new PluginManager() {

            @Override
            public <P> Iterator<P> load(Class<P> service) {
                checkArgument(service.equals(StateChangelogWriterFactory.class));
                //noinspection unchecked
                return (Iterator<P>) iterator;
            }
        };
    }
}
