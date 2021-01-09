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
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.util.Iterator;

/** Test for {@link PluginLoader}. */
public class PluginLoaderTest extends PluginTestBase {

    @Test
    public void testPluginLoading() throws Exception {

        final URL classpathA = createPluginJarURLFromString(PLUGIN_A);

        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        PluginDescriptor pluginDescriptorA =
                new PluginDescriptor("A", new URL[] {classpathA}, parentPatterns);
        ClassLoader pluginClassLoaderA =
                PluginLoader.createPluginClassLoader(
                        pluginDescriptorA, PARENT_CLASS_LOADER, new String[0]);
        Assertions.assertNotEquals(PARENT_CLASS_LOADER, pluginClassLoaderA);
        final PluginLoader pluginLoaderA = new PluginLoader(pluginClassLoaderA);

        Iterator<TestSpi> testSpiIteratorA = pluginLoaderA.load(TestSpi.class);

        Assertions.assertTrue(testSpiIteratorA.hasNext());

        TestSpi testSpiA = testSpiIteratorA.next();

        Assertions.assertFalse(testSpiIteratorA.hasNext());

        Assertions.assertNotNull(testSpiA.testMethod());

        Assertions.assertEquals(
                TestServiceA.class.getCanonicalName(), testSpiA.getClass().getCanonicalName());
        // The plugin must return the same class loader as the one used to load it.
        Assertions.assertEquals(pluginClassLoaderA, testSpiA.getClassLoader());
        Assertions.assertEquals(pluginClassLoaderA, testSpiA.getClass().getClassLoader());

        // Looks strange, but we want to ensure that those classes are not instance of each other
        // because they were
        // loaded by different classloader instances because the plugin loader uses
        // child-before-parent order.
        Assertions.assertFalse(testSpiA instanceof TestServiceA);

        // In the following we check for isolation of classes between different plugin loaders.
        final PluginLoader secondPluginLoaderA =
                PluginLoader.create(pluginDescriptorA, PARENT_CLASS_LOADER, new String[0]);

        TestSpi secondTestSpiA = secondPluginLoaderA.load(TestSpi.class).next();
        Assertions.assertNotNull(secondTestSpiA.testMethod());

        // Again, this looks strange, but we expect classes with the same name, that are not equal.
        Assertions.assertEquals(
                testSpiA.getClass().getCanonicalName(),
                secondTestSpiA.getClass().getCanonicalName());
        Assertions.assertNotEquals(testSpiA.getClass(), secondTestSpiA.getClass());
    }
}
