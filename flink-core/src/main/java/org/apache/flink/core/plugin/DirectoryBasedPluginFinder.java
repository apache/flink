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

import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is used to create a collection of {@link PluginDescriptor} based on directory
 * structure for a given plugin root folder.
 *
 * <p>The expected structure is as follows: the given plugins root folder, containing the plugins
 * folder. One plugin folder contains all resources (jar files) belonging to a plugin. The name of
 * the plugin folder becomes the plugin id.
 *
 * <pre>
 * plugins-root-folder/
 *            |------------plugin-a/ (folder of plugin a)
 *            |                |-plugin-a-1.jar (the jars containing the classes of plugin a)
 *            |                |-plugin-a-2.jar
 *            |                |-...
 *            |
 *            |------------plugin-b/
 *            |                |-plugin-b-1.jar
 *           ...               |-...
 * </pre>
 */
public class DirectoryBasedPluginFinder implements PluginFinder {

    /** Pattern to match jar files in a directory. */
    private static final String JAR_MATCHER_PATTERN = "glob:**.jar";

    /** Root directory to the plugin folders. */
    private final Path pluginsRootDir;

    /** Matcher for jar files in the filesystem of the root folder. */
    private final PathMatcher jarFileMatcher;

    public DirectoryBasedPluginFinder(Path pluginsRootDir) {
        this.pluginsRootDir = pluginsRootDir;
        this.jarFileMatcher = pluginsRootDir.getFileSystem().getPathMatcher(JAR_MATCHER_PATTERN);
    }

    @Override
    public Collection<PluginDescriptor> findPlugins() throws IOException {

        if (!Files.isDirectory(pluginsRootDir)) {
            throw new IOException(
                    "Plugins root directory [" + pluginsRootDir + "] does not exist!");
        }
        try (Stream<Path> stream = Files.list(pluginsRootDir)) {
            return stream.filter((Path path) -> Files.isDirectory(path))
                    .map(
                            FunctionUtils.uncheckedFunction(
                                    this::createPluginDescriptorForSubDirectory))
                    .collect(Collectors.toList());
        }
    }

    private PluginDescriptor createPluginDescriptorForSubDirectory(Path subDirectory)
            throws IOException {
        URL[] urls = createJarURLsFromDirectory(subDirectory);
        Arrays.sort(urls, Comparator.comparing(URL::toString));
        // TODO: This class could be extended to parse exclude-pattern from a optional text files in
        // the plugin directories.
        return new PluginDescriptor(subDirectory.getFileName().toString(), urls, new String[0]);
    }

    private URL[] createJarURLsFromDirectory(Path subDirectory) throws IOException {
        try (Stream<Path> stream = Files.list(subDirectory)) {
            URL[] urls =
                    stream.filter((Path p) -> Files.isRegularFile(p) && jarFileMatcher.matches(p))
                            .map(FunctionUtils.uncheckedFunction((Path p) -> p.toUri().toURL()))
                            .toArray(URL[]::new);

            if (urls.length < 1) {
                throw new IOException(
                        "Cannot find any jar files for plugin in directory ["
                                + subDirectory
                                + "]."
                                + " Please provide the jar files for the plugin or delete the directory.");
            }

            return urls;
        }
    }
}
