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

import org.apache.flink.shaded.guava31.com.google.common.io.Files;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A manager for dealing with all user defined resource. */
@Internal
public class ResourceManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManager.class);

    private static final String JAR_SUFFIX = "jar";
    private static final String FILE_SCHEME = "file";

    protected final Path localResourceDir;
    /** Resource infos for functions. */
    private final Map<ResourceUri, ResourceCounter> functionResourceInfos;

    private final boolean cleanLocalResource;

    protected final Map<ResourceUri, URL> resourceInfos;
    protected final MutableURLClassLoader userClassLoader;

    public static ResourceManager createResourceManager(
            URL[] urls, ClassLoader parent, ReadableConfig config) {
        MutableURLClassLoader mutableURLClassLoader =
                FlinkUserCodeClassLoaders.create(urls, parent, config);
        return new ResourceManager(config, mutableURLClassLoader);
    }

    public ResourceManager(ReadableConfig config, MutableURLClassLoader userClassLoader) {
        this(
                new Path(
                        config.get(TableConfigOptions.RESOURCES_DOWNLOAD_DIR),
                        String.format("flink-table-%s", UUID.randomUUID())),
                new HashMap<>(),
                new HashMap<>(),
                userClassLoader,
                true);
    }

    private ResourceManager(
            Path localResourceDir,
            Map<ResourceUri, URL> resourceInfos,
            Map<ResourceUri, ResourceCounter> functionResourceInfos,
            MutableURLClassLoader userClassLoader,
            boolean cleanLocalResource) {
        this.localResourceDir = localResourceDir;
        this.functionResourceInfos = functionResourceInfos;
        this.resourceInfos = resourceInfos;
        this.userClassLoader = userClassLoader;
        this.cleanLocalResource = cleanLocalResource;
    }

    /**
     * Due to anyone of the resource in list maybe fail during register, so we should stage it
     * before actual register to guarantee transaction process. If all the resources are available,
     * register them into the {@link ResourceManager}.
     */
    public void registerJarResources(List<ResourceUri> resourceUris) throws IOException {
        registerResources(
                prepareStagingResources(
                        resourceUris,
                        ResourceType.JAR,
                        true,
                        url -> {
                            try {
                                JarUtils.checkJarFile(url);
                            } catch (IOException e) {
                                throw new ValidationException(
                                        String.format("Failed to register jar resource [%s]", url),
                                        e);
                            }
                        },
                        false),
                true);
    }

    /**
     * Register a file resource into {@link ResourceManager} and return the absolute local file path
     * without the scheme.
     *
     * <p>If the file is remote, it will be copied to a local file, with file name suffixed with a
     * UUID.
     *
     * @param resourceUri resource with type as {@link ResourceType#FILE}, the resource uri might or
     *     might not contain the uri scheme, or it could be a relative path.
     * @return the absolute local file path.
     */
    public String registerFileResource(ResourceUri resourceUri) throws IOException {
        Map<ResourceUri, URL> stagingResources =
                prepareStagingResources(
                        Collections.singletonList(resourceUri),
                        ResourceType.FILE,
                        false,
                        url -> {},
                        false);
        registerResources(stagingResources, false);
        return resourceInfos.get(new ArrayList<>(stagingResources.keySet()).get(0)).getPath();
    }

    /**
     * Declare a resource for function and add it to the function resource infos. If the file is
     * remote, it will be copied to a local file. The declared resource will not be added to
     * resources and classloader if it is not used in the job.
     *
     * @param resourceUris the resource uri for function.
     */
    public void declareFunctionResources(Set<ResourceUri> resourceUris) throws IOException {
        prepareStagingResources(
                resourceUris,
                ResourceType.JAR,
                true,
                url -> {
                    try {
                        JarUtils.checkJarFile(url);
                    } catch (IOException e) {
                        throw new ValidationException(
                                String.format("Failed to register jar resource [%s]", url), e);
                    }
                },
                true);
    }

    /**
     * Unregister the resource uri in function resources, when the reference count of the resource
     * is 0, the resource will be removed from the function resources.
     *
     * @param resourceUris the uris to unregister in function resources.
     */
    public void unregisterFunctionResources(List<ResourceUri> resourceUris) {
        if (!resourceUris.isEmpty()) {
            resourceUris.forEach(
                    uri -> {
                        ResourceCounter counter = functionResourceInfos.get(uri);
                        if (counter != null && counter.decreaseCounter()) {
                            functionResourceInfos.remove(uri);
                        }
                    });
        }
    }

    public URLClassLoader getUserClassLoader() {
        return userClassLoader;
    }

    public URLClassLoader createUserClassLoader(List<ResourceUri> resourceUris) {
        if (resourceUris.isEmpty()) {
            return userClassLoader;
        }
        MutableURLClassLoader classLoader = userClassLoader.copy();
        for (ResourceUri resourceUri : new HashSet<>(resourceUris)) {
            classLoader.addURL(checkNotNull(functionResourceInfos.get(resourceUri)).url);
        }

        return classLoader;
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

    public ResourceManager copy() {
        return new ResourceManager(
                localResourceDir,
                new HashMap<>(resourceInfos),
                new HashMap<>(functionResourceInfos),
                userClassLoader.copy(),
                false);
    }

    @Override
    public void close() throws IOException {
        resourceInfos.clear();
        functionResourceInfos.clear();

        IOException exception = null;
        try {
            userClassLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing user classloader.", e);
            exception = e;
        }

        if (cleanLocalResource) {
            FileSystem fileSystem = FileSystem.getLocalFileSystem();
            try {
                if (fileSystem.exists(localResourceDir)) {
                    fileSystem.delete(localResourceDir, true);
                }
            } catch (IOException ioe) {
                LOG.debug(
                        String.format("Error while delete directory [%s].", localResourceDir), ioe);
                exception = ExceptionUtils.firstOrSuppressed(ioe, exception);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    /** Check whether the {@link Path} exists. */
    public boolean exists(Path filePath) throws IOException {
        return filePath.getFileSystem().exists(filePath);
    }

    /**
     * Generate a local file resource by the given resource generator and then synchronize to the
     * path identified by the given {@link ResourceUri}. The path passed to resource generator
     * should be a local path retrieved from the given {@link ResourceUri}.
     *
     * <p>NOTE: if the given {@link ResourceUri} represents a remote file path like
     * "hdfs://path/to/file.json", then the retrieved local path will be
     * "/localResourceDir/file-${uuid}.json"
     *
     * @param resourceUri the file resource uri to synchronize to
     * @param resourceGenerator a consumer that generates a local copy of the file resource
     */
    public void syncFileResource(ResourceUri resourceUri, Consumer<String> resourceGenerator)
            throws IOException {
        Path targetPath = new Path(resourceUri.getUri());
        String localPath;
        boolean remote = isRemotePath(targetPath);
        if (remote) {
            localPath = getResourceLocalPath(targetPath).getPath();
        } else {
            localPath = getURLFromPath(targetPath).getPath();
        }
        resourceGenerator.accept(localPath);
        if (remote) {
            if (exists(targetPath)) {
                // FileUtils#copy will not do copy if targetPath already exists
                targetPath.getFileSystem().delete(targetPath, false);
            }
            FileUtils.copy(new Path(localPath), targetPath, false);
        }
    }

    // ------------------------------------------------------------------------

    protected void checkPath(Path path, ResourceType expectedType) throws IOException {
        FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
        // check resource exists firstly
        if (!fs.exists(path)) {
            throw new FileNotFoundException(
                    String.format(
                            "%s resource [%s] not found.",
                            expectedType.name().toLowerCase(), path));
        }
        // register directory is not allowed for resource
        if (fs.getFileStatus(path).isDir()) {
            throw new ValidationException(
                    String.format(
                            "The registering or unregistering %s resource [%s] is a directory that is not allowed.",
                            expectedType.name().toLowerCase(), path));
        }

        if (expectedType == ResourceType.JAR) {
            // file name should end with .jar suffix
            String fileExtension = Files.getFileExtension(path.getName());
            if (!fileExtension.toLowerCase().endsWith(JAR_SUFFIX)) {
                throw new ValidationException(
                        String.format(
                                "The registering or unregistering jar resource [%s] must ends with '.jar' suffix.",
                                path));
            }
        }
    }

    @VisibleForTesting
    URL downloadResource(Path remotePath, boolean executable) throws IOException {
        // get a local resource path
        Path localPath = getResourceLocalPath(remotePath);
        try {
            FileUtils.copy(remotePath, localPath, executable);
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

    @VisibleForTesting
    boolean isRemotePath(Path path) {
        String scheme = path.toUri().getScheme();
        if (scheme == null) {
            // whether the default fs is configured as remote mode,
            // see FileSystem#getDefaultFsUri()
            return !FILE_SCHEME.equalsIgnoreCase(FileSystem.getDefaultFsUri().getScheme());
        } else {
            return !FILE_SCHEME.equalsIgnoreCase(scheme);
        }
    }

    @VisibleForTesting
    Map<ResourceUri, ResourceCounter> functionResourceInfos() {
        return functionResourceInfos;
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

    private void checkResources(Collection<ResourceUri> resourceUris, ResourceType expectedType)
            throws IOException {
        // check the resource type
        if (resourceUris.stream()
                .anyMatch(resourceUri -> expectedType != resourceUri.getResourceType())) {
            throw new ValidationException(
                    String.format(
                            "Expect the resource type to be %s, but encounter a resource %s.",
                            expectedType.name().toLowerCase(),
                            resourceUris.stream()
                                    .filter(
                                            resourceUri ->
                                                    expectedType != resourceUri.getResourceType())
                                    .findFirst()
                                    .map(
                                            resourceUri ->
                                                    String.format(
                                                            "[%s] with type %s",
                                                            resourceUri.getUri(),
                                                            resourceUri
                                                                    .getResourceType()
                                                                    .name()
                                                                    .toLowerCase()))
                                    .get()));
        }

        // check the resource path
        for (ResourceUri resourceUri : resourceUris) {
            checkPath(new Path(resourceUri.getUri()), expectedType);
        }
    }

    private Map<ResourceUri, URL> prepareStagingResources(
            Collection<ResourceUri> resourceUris,
            ResourceType expectedType,
            boolean executable,
            Consumer<URL> resourceChecker,
            boolean declareFunctionResource)
            throws IOException {
        checkResources(resourceUris, expectedType);

        Map<ResourceUri, URL> stagingResourceLocalURLs = new HashMap<>();
        boolean supportOverwrite = !executable;
        for (ResourceUri resourceUri : resourceUris) {
            // check whether the resource has been registered
            if (resourceInfos.containsKey(resourceUri) && resourceInfos.get(resourceUri) != null) {
                if (!supportOverwrite) {
                    LOG.info(
                            "Resource [{}] has been registered, overwriting of registered resource is not supported "
                                    + "in the current version, skipping.",
                            resourceUri.getUri());
                    continue;
                }
            }

            URL localUrl;
            ResourceUri localResourceUri = resourceUri;
            if (expectedType == ResourceType.JAR
                    && functionResourceInfos.containsKey(resourceUri)) {
                // Get local url from function resource infos.
                localUrl = functionResourceInfos.get(resourceUri).url;
                // Register resource uri to increase the reference counter
                functionResourceInfos
                        .computeIfAbsent(resourceUri, key -> new ResourceCounter(localUrl))
                        .increaseCounter();
            } else {
                // here can check whether the resource path is valid
                Path path = new Path(resourceUri.getUri());
                // download resource to a local path firstly if in remote
                if (isRemotePath(path)) {
                    localUrl = downloadResource(path, executable);
                } else {
                    localUrl = getURLFromPath(path);
                    // if the local resource is a relative path, here convert it to an absolute path
                    // before register
                    localResourceUri = new ResourceUri(expectedType, localUrl.getPath());
                }

                // check the local file
                resourceChecker.accept(localUrl);

                if (declareFunctionResource) {
                    functionResourceInfos
                            .computeIfAbsent(resourceUri, key -> new ResourceCounter(localUrl))
                            .increaseCounter();
                }
            }

            // add it to a staging map
            stagingResourceLocalURLs.put(localResourceUri, localUrl);
        }
        return stagingResourceLocalURLs;
    }

    private void registerResources(
            Map<ResourceUri, URL> stagingResources, boolean addToClassLoader) {
        // register resource in batch
        stagingResources.forEach(
                (resourceUri, url) -> {
                    if (addToClassLoader) {
                        userClassLoader.addURL(url);
                        LOG.info(
                                "Added {} resource [{}] to class path.",
                                resourceUri.getResourceType().name(),
                                url);
                    }
                    resourceInfos.put(resourceUri, url);
                    LOG.info("Register resource [{}] successfully.", resourceUri.getUri());
                });
    }

    /**
     * Resource with reference counter, when the counter is 0, it means the resource can be removed.
     */
    static class ResourceCounter {
        final URL url;
        int counter;

        private ResourceCounter(URL url) {
            this.url = url;
            this.counter = 0;
        }

        private void increaseCounter() {
            this.counter++;
        }

        private boolean decreaseCounter() {
            this.counter--;
            checkState(
                    this.counter >= 0,
                    String.format("Invalid reference count[%d] which must >= 0", this.counter));
            return this.counter == 0;
        }
    }
}
