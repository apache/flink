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

import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

/**
 * Test for {@link DirectoryBasedPluginFinder}.
 */
public class DirectoryBasedPluginFinderTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void createPluginDescriptorsForDirectory() throws Exception {
		File rootFolder = temporaryFolder.newFolder();
		PluginFinder descriptorsFactory =
			new DirectoryBasedPluginFinder(rootFolder.toPath());
		Collection<PluginDescriptor> actual = descriptorsFactory.findPlugins();

		Assert.assertTrue("empty root dir -> expected no actual", actual.isEmpty());

		List<File> subDirs = Stream.of("A", "B", "C")
			.map(s -> new File(rootFolder, s))
			.collect(Collectors.toList());

		for (File subDir : subDirs) {
			Preconditions.checkState(subDir.mkdirs());
		}

		try {
			descriptorsFactory.findPlugins();
			fail("all empty plugin sub-dirs");
		} catch (RuntimeException expected) {
			Assert.assertTrue(expected.getCause() instanceof IOException);
		}

		for (File subDir : subDirs) {
			// we create a file and another subfolder to check that they are ignored
			Preconditions.checkState(new File(subDir, "ignore-test.zip").createNewFile());
			Preconditions.checkState(new File(subDir, "ignore-dir").mkdirs());
		}

		try {
			descriptorsFactory.findPlugins();
			fail("still no jars in plugin sub-dirs");
		} catch (RuntimeException expected) {
			Assert.assertTrue(expected.getCause() instanceof IOException);
		}

		List<PluginDescriptor> expected = new ArrayList<>(3);

		for (int i = 0; i < subDirs.size(); ++i) {
			File subDir = subDirs.get(i);
			URL[] jarURLs = new URL[i + 1];

			for (int j = 0; j <= i; ++j) {
				File file = new File(subDir, "jar-file-" + j + ".jar");
				Preconditions.checkState(file.createNewFile());
				jarURLs[j] = file.toURI().toURL();
			}

			Arrays.sort(jarURLs, Comparator.comparing(URL::toString));
			expected.add(new PluginDescriptor(subDir.getName(), jarURLs, new String[0]));
		}

		actual = descriptorsFactory.findPlugins();

		Assert.assertTrue(equalsIgnoreOrder(expected, new ArrayList<>(actual)));
	}

	private boolean equalsIgnoreOrder(List<PluginDescriptor> a, List<PluginDescriptor> b) {

		if (a.size() != b.size()) {
			return false;
		}

		final Comparator<PluginDescriptor> comparator = Comparator.comparing(PluginDescriptor::getPluginId);

		a.sort(comparator);
		b.sort(comparator);

		final Iterator<PluginDescriptor> iterA = a.iterator();
		final Iterator<PluginDescriptor> iterB = b.iterator();

		while (iterA.hasNext()) {
			if (!equals(iterA.next(), iterB.next())) {
				return false;
			}
		}
		return true;
	}

	private static boolean equals(@Nonnull PluginDescriptor a, @Nonnull PluginDescriptor b) {
		return a.getPluginId().equals(b.getPluginId())
			&& Arrays.deepEquals(a.getPluginResourceURLs(), b.getPluginResourceURLs())
			&& Arrays.deepEquals(a.getLoaderExcludePatterns(), b.getLoaderExcludePatterns());
	}
}
