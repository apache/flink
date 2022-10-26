/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.resource;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.JarUtils;
import org.apache.flink.util.MutableURLClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.io.Files;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** A manager for dealing with all user defined resource. */
@Internal
public class ResourceManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManager.class);

    private static final String JAR_SUFFIX = "jar";
    private static final String FILE_SCHEME = "file";

    private final Path localResourceDir;
    protected final Map<ResourceUri, URL> resourceInfos;
    protected final MutableURLClassLoader userClassLoader;

    public static ResourceManager createResourceManager(
            URL[] urls, ClassLoader parent, ReadableConfig config) {
        MutableURLClassLoader mutableURLClassLoader =
                FlinkUserCodeClassLoaders.create(urls, parent, config);
        return new ResourceManager(config, mutableURLClassLoader);
    }

    public ResourceManager(ReadableConfig config, MutableURLClassLoader userClassLoader) {
        this.localResourceDir =
                new Path(
                        config.get(TableConfigOptions.RESOURCES_DOWNLOAD_DIR),
                        String.format("flink-table-%s", UUID.randomUUID()));
        this.resourceInfos = new HashMap<>();
        this.userClassLoader = userClassLoader;
    }

    /**
     * Due to anyone of the resource in list maybe fail during register, so we should stage it
     * before actual register to guarantee transaction process. If all the resources are available,
     * register them into the {@link ResourceManager}.
     */
    public void registerJarResources(List<ResourceUri> resourceUris) throws IOException {
        // check jar resource before register
        checkJarResources(resourceUris);

        Map<ResourceUri, URL> stagingResourceLocalURLs = new HashMap<>();
        for (ResourceUri resourceUri : resourceUris) {
            // check whether the resource has been registered
            if (resourceInfos.containsKey(resourceUri) && resourceInfos.get(resourceUri) != null) {
                LOG.info(
                        "Resource [{}] has been registered, overwriting of registered resource is not supported "
                                + "in the current version, skipping.",
                        resourceUri.getUri());
                continue;
            }

            // here can check whether the resource path is valid
            Path path = new Path(resourceUri.getUri());
            URL localUrl;
            // check resource scheme
            String scheme = StringUtils.lowerCase(path.toUri().getScheme());
            // download resource to local path firstly if in remote
            if (scheme != null && !FILE_SCHEME.equals(scheme)) {
                localUrl = downloadResource(path);
            } else {
                localUrl = getURLFromPath(path);
                // if the local jar resource is a relative path, here convert it to absolute path
                // before register
                resourceUri = new ResourceUri(ResourceType.JAR, localUrl.getPath());
            }

            // check the local jar file
            JarUtils.checkJarFile(localUrl);

            // add it to staging map
            stagingResourceLocalURLs.put(resourceUri, localUrl);
        }

        // register resource in batch
        stagingResourceLocalURLs.forEach(
                (resourceUri, url) -> {
                    // jar resource need add to classloader
                    userClassLoader.addURL(url);
                    LOG.info("Added jar resource [{}] to class path.", url);

                    resourceInfos.put(resourceUri, url);
                    LOG.info("Register resource [{}] successfully.", resourceUri.getUri());
                });
    }

    public URLClassLoader getUserClassLoader() {
        return userClassLoader;
    }

    public Map<ResourceUri, URL> getResources() {
        return Collections.unmodifiableMap(resourceInfos);
    }

    /**
     * Get the local jars' URL. Return the URL corresponding to downloaded jars in the local file
     * system for the remote jar. For the local jar, return the registered URL.
     */
    public Set<URL> getLocalJarResources() {
        return resourceInfos.entrySet().stream()
                .filter(entry -> ResourceType.JAR.equals(entry.getKey().getResourceType()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
    }

    /**
     * Adds the local jar resources to the given {@link TableConfig}. It implicitly considers the
     * {@link TableConfig#getRootConfiguration()} and stores the merged result into {@link
     * TableConfig#getConfiguration()}.
     */
    public void addJarConfiguration(TableConfig tableConfig) {
        final List<String> jars =
                getLocalJarResources().stream().map(URL::toString).collect(Collectors.toList());
        if (jars.isEmpty()) {
            return;
        }
        final Set<String> jarFiles =
                tableConfig
                        .getOptional(PipelineOptions.JARS)
                        .map(LinkedHashSet::new)
                        .orElseGet(LinkedHashSet::new);
        jarFiles.addAll(jars);
        tableConfig.set(PipelineOptions.JARS, new ArrayList<>(jarFiles));
    }

    @Override
    public void close() throws IOException {
        resourceInfos.clear();

        IOException exception = null;
        try {
            userClassLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing user classloader.", e);
            exception = e;
        }

        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        try {
            if (fileSystem.exists(localResourceDir)) {
                fileSystem.delete(localResourceDir, true);
            }
        } catch (IOException ioe) {
            LOG.debug(String.format("Error while delete directory [%s].", localResourceDir), ioe);
            exception = ExceptionUtils.firstOrSuppressed(ioe, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    private void checkJarResources(List<ResourceUri> resourceUris) throws IOException {
        // only support register jar resource
        if (resourceUris.stream()
                .anyMatch(resourceUri -> !ResourceType.JAR.equals(resourceUri.getResourceType()))) {
            throw new ValidationException(
                    String.format(
                            "Only support to register jar resource, resource info:\n %s.",
                            resourceUris.stream()
                                    .map(ResourceUri::getUri)
                                    .collect(Collectors.joining(",\n"))));
        }

        for (ResourceUri resourceUri : resourceUris) {
            checkJarPath(new Path(resourceUri.getUri()));
        }
    }

    protected void checkJarPath(Path path) throws IOException {
        // file name should end with .jar suffix
        String fileExtension = Files.getFileExtension(path.getName());
        if (!fileExtension.toLowerCase().endsWith(JAR_SUFFIX)) {
            throw new ValidationException(
                    String.format(
                            "The registering or unregistering jar resource [%s] must ends with '.jar' suffix.",
                            path));
        }

        FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
        // check resource exists firstly
        if (!fs.exists(path)) {
            throw new FileNotFoundException(String.format("Jar resource [%s] not found.", path));
        }

        // register directory is not allowed for resource
        if (fs.getFileStatus(path).isDir()) {
            throw new ValidationException(
                    String.format(
                            "The registering or unregistering jar resource [%s] is a directory that is not allowed.",
                            path));
        }
    }

    @VisibleForTesting
    URL downloadResource(Path remotePath) throws IOException {
        // get local resource path
        Path localPath = getResourceLocalPath(remotePath);
        try {
            FileUtils.copy(remotePath, localPath, true);
            LOG.info(
                    "Download resource [{}] to local path [{}] successfully.",
                    remotePath,
                    localPath);
        } catch (IOException e) {
            throw new IOException(
                    String.format(
                            "Failed to download resource [%s] to local path [%s].",
                            remotePath, localPath),
                    e);
        }
        return getURLFromPath(localPath);
    }

    @VisibleForTesting
    protected URL getURLFromPath(Path path) throws IOException {
        // if scheme is null, rewrite it to file
        if (path.toUri().getScheme() == null) {
            path = path.makeQualified(FileSystem.getLocalFileSystem());
        }
        return path.toUri().toURL();
    }

    @VisibleForTesting
    Path getLocalResourceDir() {
        return localResourceDir;
    }

    private Path getResourceLocalPath(Path remotePath) {
        String fileName = remotePath.getName();
        String fileExtension = Files.getFileExtension(fileName);
        // add UUID suffix to avoid conflicts
        String fileNameWithUUID;
        if (StringUtils.isEmpty(fileExtension)) {
            fileNameWithUUID = String.format("%s-%s", fileName, UUID.randomUUID());
        } else {
            fileNameWithUUID =
                    String.format(
                            "%s-%s.%s",
                            Files.getNameWithoutExtension(fileName),
                            UUID.randomUUID(),
                            fileExtension);
        }
        return new Path(localResourceDir, fileNameWithUUID);
    }
}
