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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.util.FileUtils;
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
import java.util.Collections;
import java.util.HashMap;
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

    public ResourceManager(Configuration config, MutableURLClassLoader userClassLoader) {
        this.localResourceDir =
                new Path(
                        config.get(TableConfigOptions.RESOURCE_DOWNLOAD_DIR),
                        String.format("flink-table-%s", UUID.randomUUID()));
        this.resourceInfos = new HashMap<>();
        this.userClassLoader = userClassLoader;
    }

    public URLClassLoader getUserClassLoader() {
        return userClassLoader;
    }

    /**
     * Due to anyone of the resource in list maybe fail during register, so we should stage it
     * before actual register to guarantee transaction process. If all the resources are avaliable,
     * register them into the {@link ResourceManager}.
     */
    public void registerJarResource(List<ResourceUri> resourceUris) throws IOException {
        // check jar resource before register
        checkJarResource(resourceUris);

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

            // check the local jar file extra
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

    public Map<ResourceUri, URL> getResources() {
        return Collections.unmodifiableMap(resourceInfos);
    }

    public Set<URL> getJarResourceURLs() {
        return resourceInfos.entrySet().stream()
                .filter(entry -> ResourceType.JAR.equals(entry.getKey().getResourceType()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
    }

    private void checkJarResource(List<ResourceUri> resourceUris) throws IOException {
        // only support register jar resource
        if (resourceUris.stream()
                .anyMatch(resourceUri -> !ResourceType.JAR.equals(resourceUri.getResourceType()))) {
            throw new IOException(
                    String.format(
                            "Only support to register jar resource, resource info:\n %s.",
                            resourceUris.stream()
                                    .map(ResourceUri::getUri)
                                    .collect(Collectors.joining(",\n"))));
        }

        for (ResourceUri resourceUri : resourceUris) {
            // here can check whether the resource path is valid
            Path path = new Path(resourceUri.getUri());
            // file name should end with .jar suffix
            String fileExtension = Files.getFileExtension(path.getName());
            if (!fileExtension.toLowerCase().endsWith(JAR_SUFFIX)) {
                throw new IOException(
                        String.format(
                                "The registering jar resource [%s] must ends with '.jar' suffix.",
                                path));
            }

            FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
            // check resource exists firstly
            if (!fs.exists(path)) {
                throw new FileNotFoundException(
                        String.format("Jar resource [%s] not found.", path));
            }

            // register directory is not allowed for resource
            if (fs.getFileStatus(path).isDir()) {
                throw new IOException(
                        String.format(
                                "The registering jar resource [%s] is a directory, however directory is not allowed to register.",
                                path));
            }
        }
    }

    @VisibleForTesting
    protected URL downloadResource(Path remotePath) throws IOException {
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

    @VisibleForTesting
    protected URL getURLFromPath(Path path) throws IOException {
        // if scheme is null, rewrite it to file
        if (path.toUri().getScheme() == null) {
            path = path.makeQualified(FileSystem.getLocalFileSystem());
        }
        return path.toUri().toURL();
    }

    @Override
    public void close() throws IOException {
        // clear the map
        resourceInfos.clear();

        IOException exception = null;
        // close classloader
        try {
            userClassLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing user classloader.", e);
            exception = e;
        }
        // delete the local resource
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        try {
            if (fileSystem.exists(localResourceDir)) {
                fileSystem.delete(localResourceDir, true);
            }
        } catch (IOException e) {
            LOG.debug(String.format("Error while delete directory [%s].", localResourceDir), e);
            if (exception == null) {
                exception = e;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }
}
