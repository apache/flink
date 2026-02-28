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

import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginLoader;
import org.apache.flink.test.plugin.jar.plugina.TestServiceA;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PluginLoader}. */
class PluginLoaderTest extends PluginTestBase {

    @Test
    void testPluginLoading() throws Exception {
        final URL classpathA = createPluginJarURLFromString(PLUGIN_A);

        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        PluginDescriptor pluginDescriptorA =
                new PluginDescriptor("A", new URL[] {classpathA}, parentPatterns);
        URLClassLoader pluginClassLoaderA =
                PluginLoader.createPluginClassLoader(
                        pluginDescriptorA, PARENT_CLASS_LOADER, new String[0]);
        assertThat(pluginClassLoaderA).isNotEqualTo(PARENT_CLASS_LOADER);
        final PluginLoader pluginLoaderA = new PluginLoader("test-plugin", pluginClassLoaderA);

        Iterator<TestSpi> testSpiIteratorA = pluginLoaderA.load(TestSpi.class);

        assertThat(testSpiIteratorA.hasNext()).isTrue();

        TestSpi testSpiA = testSpiIteratorA.next();

        assertThat(testSpiIteratorA.hasNext()).isFalse();

        assertThat(testSpiA.testMethod()).isNotNull();

        assertThat(testSpiA.getClass().getCanonicalName())
                .isEqualTo(TestServiceA.class.getCanonicalName());
        // The plugin must return the same class loader as the one used to load it.
        assertThat(testSpiA.getClassLoader()).isEqualTo(pluginClassLoaderA);
        assertThat(testSpiA.getClass().getClassLoader()).isEqualTo(pluginClassLoaderA);

        // Looks strange, but we want to ensure that those classes are not instance of each other
        // because they were
        // loaded by different classloader instances because the plugin loader uses
        // child-before-parent order.
        assertThat(testSpiA).isNotInstanceOf(TestServiceA.class);

        // In the following we check for isolation of classes between different plugin loaders.
        final PluginLoader secondPluginLoaderA =
                PluginLoader.create(pluginDescriptorA, PARENT_CLASS_LOADER, new String[0]);

        TestSpi secondTestSpiA = secondPluginLoaderA.load(TestSpi.class).next();
        assertThat(secondTestSpiA.testMethod()).isNotNull();

        // Again, this looks strange, but we expect classes with the same name, that are not equal.
        assertThat(secondTestSpiA.getClass().getCanonicalName())
                .isEqualTo(testSpiA.getClass().getCanonicalName());
        assertThat(secondTestSpiA.getClass()).isNotEqualTo(testSpiA.getClass());
    }

    @Test
    void testClose() throws MalformedURLException {
        final URL classpathA = createPluginJarURLFromString(PLUGIN_A);

        String[] parentPatterns = {TestSpi.class.getName()};
        PluginDescriptor pluginDescriptorA =
                new PluginDescriptor("A", new URL[] {classpathA}, parentPatterns);
        URLClassLoader pluginClassLoaderA =
                PluginLoader.createPluginClassLoader(
                        pluginDescriptorA, PARENT_CLASS_LOADER, new String[0]);

        final PluginLoader pluginLoaderA = new PluginLoader("test-plugin", pluginClassLoaderA);
        pluginLoaderA.close();

        assertThatThrownBy(() -> pluginClassLoaderA.loadClass(junit.framework.Test.class.getName()))
                .isInstanceOf(ClassNotFoundException.class);
    }
}
