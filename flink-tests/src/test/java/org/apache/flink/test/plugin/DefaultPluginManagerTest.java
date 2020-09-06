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
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginFinder;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Test for {@link DefaultPluginManager}.
 */
public class DefaultPluginManagerTest extends PluginTestBase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
		final PluginFinder descriptorsFactory = new DirectoryBasedPluginFinder(pluginRootFolderPath);
		descriptors = descriptorsFactory.findPlugins();
		Preconditions.checkState(descriptors.size() == 2);
	}

	@Test
	public void testPluginLoading() {

		String[] parentPatterns = { TestSpi.class.getName(), OtherTestSpi.class.getName() };
		final PluginManager pluginManager = new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
		final List<TestSpi> serviceImplList = Lists.newArrayList(pluginManager.load(TestSpi.class));
		Assert.assertEquals(2, serviceImplList.size());

		// check that all impl have unique classloader
		final Set<ClassLoader> classLoaders = Collections.newSetFromMap(new IdentityHashMap<>(3));
		classLoaders.add(PARENT_CLASS_LOADER);
		for (TestSpi testSpi : serviceImplList) {
			Assert.assertNotNull(testSpi.testMethod());
			Assert.assertTrue(classLoaders.add(testSpi.getClass().getClassLoader()));
		}

		final List<OtherTestSpi> otherServiceImplList = Lists.newArrayList(pluginManager.load(OtherTestSpi.class));
		Assert.assertEquals(1, otherServiceImplList.size());
		for (OtherTestSpi otherTestSpi : otherServiceImplList) {
			Assert.assertNotNull(otherTestSpi.otherTestMethod());
			Assert.assertTrue(classLoaders.add(otherTestSpi.getClass().getClassLoader()));
		}
	}
}
