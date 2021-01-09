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

package org.apache.flink.test.plugin;

import org.apache.flink.core.plugin.*;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Preconditions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** Test for {@link DefaultPluginManager}. */
public class DefaultPluginManagerTest extends PluginTestBase {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Collection<PluginDescriptor> descriptors;

    @Before
    public void setup() throws Exception {
        /*
         * We setup a plugin directory hierarchy and utilize DirectoryBasedPluginFinder to create the
         * descriptors:
         *
         * <pre>
         * tmp/plugins-root/
         *          |-------------A/
         *          |             |-plugin-a.jar
         *          |
         *          |-------------B/
         *                        |-plugin-b.jar
         * </pre>
         */
        final File pluginRootFolder = temporaryFolder.newFolder();
        final Path pluginRootFolderPath = pluginRootFolder.toPath();
        final File pluginAFolder = new File(pluginRootFolder, "A");
        final File pluginBFolder = new File(pluginRootFolder, "B");
        Preconditions.checkState(pluginAFolder.mkdirs());
        Preconditions.checkState(pluginBFolder.mkdirs());
        Files.copy(locateJarFile(PLUGIN_A).toPath(), Paths.get(pluginAFolder.toString(), PLUGIN_A));
        Files.copy(locateJarFile(PLUGIN_B).toPath(), Paths.get(pluginBFolder.toString(), PLUGIN_B));
        final PluginFinder descriptorsFactory =
                new DirectoryBasedPluginFinder(pluginRootFolderPath);
        descriptors = descriptorsFactory.findPlugins();
        Preconditions.checkState(descriptors.size() == 2);
    }

    @Test
    public void testPluginLoading() {

        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        final PluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
        final List<TestSpi> serviceImplList = Lists.newArrayList(pluginManager.load(TestSpi.class));
        Assertions.assertEquals(2, serviceImplList.size());

        // check that all impl have unique classloader
        final Set<ClassLoader> classLoaders = Collections.newSetFromMap(new IdentityHashMap<>(3));
        classLoaders.add(PARENT_CLASS_LOADER);
        for (TestSpi testSpi : serviceImplList) {
            Assertions.assertNotNull(testSpi.testMethod());
            Assertions.assertTrue(classLoaders.add(testSpi.getClass().getClassLoader()));
        }

        final List<OtherTestSpi> otherServiceImplList =
                Lists.newArrayList(pluginManager.load(OtherTestSpi.class));
        Assertions.assertEquals(1, otherServiceImplList.size());
        for (OtherTestSpi otherTestSpi : otherServiceImplList) {
            Assertions.assertNotNull(otherTestSpi.otherTestMethod());
            Assertions.assertTrue(classLoaders.add(otherTestSpi.getClass().getClassLoader()));
        }
    }
}
