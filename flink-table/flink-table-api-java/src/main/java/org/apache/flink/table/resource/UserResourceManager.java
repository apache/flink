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
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader;

/** A manager for dealing with all user defined resource. */
@Internal
public class UserResourceManager {

    private final Logger LOG = LoggerFactory.getLogger(UserResourceManager.class);

    private final Configuration config;
    private final SafetyNetWrapperClassLoader userClassLoader;
    private final Map<ResourceUri, URL> resourceInfos;

    public UserResourceManager(Configuration config, SafetyNetWrapperClassLoader userClassLoader) {
        this.config = config;
        this.userClassLoader = userClassLoader;
        this.resourceInfos = new HashMap<>();
    }

    public URLClassLoader getUserClassLoader() {
        return userClassLoader;
    }

    public void registerResource(ResourceUri resourceUri) {
        // check whether the resource has been registered
        if (resourceInfos.containsKey(resourceUri)) {
            LOG.info(
                    "Resource [{}] has been registered, overwriting of registered resource is not supported "
                            + "in the current version, skipping.",
                    resourceUri.getUri());
            return;
        }

        Path path = new Path(resourceUri.getUri());
        // check resource firstly
        checkResource(resourceUri.getResourceType(), path);

        URL localUrl;
        // check resource scheme
        String scheme = StringUtils.lowerCase(path.toUri().getScheme());
        // download resource to local path firstly if in remote
        if (scheme != null && !"file".equals(scheme)) {
            localUrl = downloadResource(path);
        } else {
            localUrl = getURLFromPath(path);
        }

        // only need add jar resource to classloader
        if (ResourceType.JAR.equals(resourceUri.getResourceType())) {
            userClassLoader.addURL(localUrl);
            LOG.info("Added jar resource [{}] to class path.", localUrl.toString());
        }

        resourceInfos.put(resourceUri, localUrl);
        LOG.info("Register resource [{}] successfully.", resourceUri.getUri());
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

    private void checkResource(ResourceType resourceType, Path path) {
        try {
            FileSystem fs = path.getFileSystem();
            // check resource exists firstly
            if (!fs.exists(path)) {
                throw new IllegalArgumentException(String.format("Resource [%s] not found.", path));
            }

            // register directory is not allowed for jar resource
            if (ResourceType.JAR.equals(resourceType) && fs.getFileStatus(path).isDir()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Directory [%s] is not allowed for registering jar resource.",
                                path));
            }

        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to check resource [%s].", path), e);
        }
    }

    @VisibleForTesting
    protected URL downloadResource(Path remotePath) {
        final String localTmpDir = config.get(TableConfigOptions.DOWNLOADED_RESOURCES_TMP_DIR);
        // add an uuid prefix for local file name
        Path localPath =
                new Path(
                        String.format(
                                "%s%s%s-%s",
                                localTmpDir,
                                Path.SEPARATOR,
                                UUID.randomUUID(),
                                remotePath.getName()));
        try {
            FileUtils.copy(remotePath, localPath, true);
            LOG.info(
                    "Download resource [{}] to local path [{}] successfully.",
                    remotePath,
                    localPath);
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to download resource [%s] to local path [%s].",
                            remotePath, localTmpDir),
                    e);
        }
        return getURLFromPath(localPath);
    }

    @VisibleForTesting
    protected URL getURLFromPath(Path path) {
        try {
            // if scheme is null, rewrite it to file
            if (path.toUri().getScheme() == null) {
                path = path.makeQualified(FileSystem.getLocalFileSystem());
            }
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to convert the resource path [%s] to URL.", path), e);
        }
    }
}
