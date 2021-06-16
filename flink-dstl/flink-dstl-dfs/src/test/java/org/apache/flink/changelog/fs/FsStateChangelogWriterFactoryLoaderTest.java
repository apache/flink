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

package org.apache.flink.changelog.fs;

import org.apache.flink.core.plugin.DefaultPluginManager;
import org.apache.flink.runtime.state.changelog.StateChangelogWriterFactory;
import org.apache.flink.runtime.state.changelog.StateChangelogWriterFactoryLoader;

import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.fail;

/**
 * {@link StateChangelogWriterFactoryLoader} test for {@link FsStateChangelogWriterFactory} case.
 */
public class FsStateChangelogWriterFactoryLoaderTest {

    @Test
    @SuppressWarnings("rawtypes")
    public void testLoaded() {
        StateChangelogWriterFactoryLoader loader =
                new StateChangelogWriterFactoryLoader(
                        new DefaultPluginManager(Collections.emptyList(), new String[0]));
        for (Iterator<StateChangelogWriterFactory> it = loader.load(); it.hasNext(); ) {
            if (it.next() instanceof FsStateChangelogWriterFactory) {
                return; // found
            }
        }
        fail(FsStateChangelogWriterFactory.class.getName() + " not loaded");
    }
}
