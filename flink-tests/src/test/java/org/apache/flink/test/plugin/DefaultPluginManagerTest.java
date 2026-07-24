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

import org.apache.flink.core.plugin.DefaultPluginManager;
import org.apache.flink.core.plugin.DirectoryBasedPluginFinder;
import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.core.plugin.PluginContext;
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginFinder;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.test.plugin.jar.pluginb.TestServiceB;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultPluginManager}. */
class DefaultPluginManagerTest extends PluginTestBase {

    @TempDir private File temporaryFolder;

    private Collection<PluginDescriptor> descriptors;

    @BeforeEach
    void setup() throws Exception {
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
        final Path pluginRootFolderPath = temporaryFolder.toPath();
        final File pluginAFolder = new File(temporaryFolder, "A");
        final File pluginBFolder = new File(temporaryFolder, "B");
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
    void testPluginLoading() {
        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        final PluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
        final List<TestSpi> serviceImplList = Lists.newArrayList(pluginManager.load(TestSpi.class));
        assertThat(serviceImplList).hasSize(2);

        // check that all impl have unique classloader
        final Set<ClassLoader> classLoaders = Collections.newSetFromMap(new IdentityHashMap<>(3));
        classLoaders.add(PARENT_CLASS_LOADER);
        for (TestSpi testSpi : serviceImplList) {
            assertThat(testSpi.testMethod()).isNotNull();
            assertThat(classLoaders.add(testSpi.getClass().getClassLoader())).isTrue();
        }

        final List<OtherTestSpi> otherServiceImplList =
                Lists.newArrayList(pluginManager.load(OtherTestSpi.class));
        assertThat(otherServiceImplList).hasSize(1);
        for (OtherTestSpi otherTestSpi : otherServiceImplList) {
            assertThat(otherTestSpi.otherTestMethod()).isNotNull();
            assertThat(classLoaders.add(otherTestSpi.getClass().getClassLoader())).isFalse();
        }
    }

    @Test
    void testLoadReturnsSingletonInstances() {
        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);

        final List<TestSpi> firstLoad = Lists.newArrayList(pluginManager.load(TestSpi.class));
        final List<TestSpi> secondLoad = Lists.newArrayList(pluginManager.load(TestSpi.class));

        assertThat(firstLoad).hasSize(2);
        assertThat(secondLoad).hasSize(2);

        final Set<TestSpi> firstLoadIdentitySet =
                Collections.newSetFromMap(new IdentityHashMap<>());
        firstLoadIdentitySet.addAll(firstLoad);
        for (TestSpi instance : secondLoad) {
            assertThat(firstLoadIdentitySet).contains(instance);
        }
    }

    @Test
    void testOpenDispatchesContextToCachedPlugins() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);

        final TrackingPlugin plugin = new TrackingPlugin();
        pluginManager.injectForTesting(Plugin.class, plugin);

        final PluginContext context = new PluginContext("test-task-manager-id");
        pluginManager.open(context);

        assertThat(plugin.openCalled).isTrue();
        assertThat(plugin.openContext).isSameAs(context);
    }

    @Test
    void testCloseDispatchesToCachedPlugins() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);

        final TrackingPlugin plugin = new TrackingPlugin();
        pluginManager.injectForTesting(Plugin.class, plugin);

        pluginManager.close();

        assertThat(plugin.closeCalled).isTrue();
    }

    @Test
    void testOpenAndCloseOnEmptyCache() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);
        final PluginContext context = new PluginContext("test-task-manager-id");
        // should complete without exception when cache is empty
        pluginManager.open(context);
        pluginManager.close();
    }

    @Test
    void testOpenAndCloseDispatchToAllCachedInstances() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);

        final TrackingPlugin pluginA = new TrackingPlugin();
        final TrackingPlugin pluginB = new TrackingPlugin();
        pluginManager.injectForTesting(Plugin.class, pluginA);
        pluginManager.injectForTesting(Plugin.class, pluginB);

        final PluginContext context = new PluginContext("test-task-manager-id");
        pluginManager.open(context);

        assertThat(pluginA.openCalled).isTrue();
        assertThat(pluginA.openContext).isSameAs(context);
        assertThat(pluginB.openCalled).isTrue();
        assertThat(pluginB.openContext).isSameAs(context);

        pluginManager.close();

        assertThat(pluginA.closeCalled).isTrue();
        assertThat(pluginB.closeCalled).isTrue();
    }

    @Test
    void testOpenSkipsNonPluginCachedInstances() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);
        pluginManager.injectForTesting(String.class, "not-a-plugin");
        // Should not throw even though the cached instance does not implement Plugin
        pluginManager.open(new PluginContext("tm-id"));
    }

    @Test
    void testCloseSkipsNonPluginCachedInstances() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);
        pluginManager.injectForTesting(String.class, "not-a-plugin");
        // Should not throw even though the cached instance does not implement Plugin
        pluginManager.close();
    }

    @Test
    void testOpenAndCloseAcrossDistinctServiceTypes() {
        final DefaultPluginManager pluginManager =
                new DefaultPluginManager(Collections.emptyList(), new String[0]);

        final PluginForServiceA pluginA = new PluginForServiceA();
        final PluginForServiceB pluginB = new PluginForServiceB();
        pluginManager.injectForTesting(ServiceA.class, pluginA);
        pluginManager.injectForTesting(ServiceB.class, pluginB);

        final PluginContext context = new PluginContext("tm-multi");
        pluginManager.open(context);

        assertThat(pluginA.openCalled).isTrue();
        assertThat(pluginA.openContext).isSameAs(context);
        assertThat(pluginB.openCalled).isTrue();
        assertThat(pluginB.openContext).isSameAs(context);

        pluginManager.close();

        assertThat(pluginA.closeCalled).isTrue();
        assertThat(pluginB.closeCalled).isTrue();
    }

    private interface ServiceA extends Plugin {}

    private interface ServiceB extends Plugin {}

    private static class PluginForServiceA implements ServiceA {
        boolean openCalled;
        PluginContext openContext;
        boolean closeCalled;

        @Override
        public void open(PluginContext ctx) {
            openCalled = true;
            openContext = ctx;
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    private static class PluginForServiceB implements ServiceB {
        boolean openCalled;
        PluginContext openContext;
        boolean closeCalled;

        @Override
        public void open(PluginContext ctx) {
            openCalled = true;
            openContext = ctx;
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    private static class TrackingPlugin implements Plugin {
        boolean openCalled;
        PluginContext openContext;
        boolean closeCalled;

        @Override
        public void open(PluginContext context) {
            openCalled = true;
            openContext = context;
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    @Test
    void classLoaderMustBeTheSameInsideAPlugin() {
        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        final PluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
        final List<TestSpi> serviceImplList = Lists.newArrayList(pluginManager.load(TestSpi.class));
        assertThat(serviceImplList).hasSize(2);

        final List<OtherTestSpi> otherServiceImplList =
                Lists.newArrayList(pluginManager.load(OtherTestSpi.class));
        assertThat(otherServiceImplList).hasSize(1);

        // instanceof with multiple classloaders works only this way
        final List<TestSpi> serviceBImplList =
                serviceImplList.stream()
                        .filter(s -> s.getClass().getName().equals(TestServiceB.class.getName()))
                        .collect(Collectors.toList());
        assertThat(serviceBImplList).hasSize(1);
        assertThat(serviceBImplList.get(0).getClass().getClassLoader())
                .isSameAs(otherServiceImplList.get(0).getClass().getClassLoader());
    }
}
