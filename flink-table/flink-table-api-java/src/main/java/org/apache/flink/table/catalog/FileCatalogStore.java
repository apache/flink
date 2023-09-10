/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link CatalogStore} that stores all catalog configuration to a directory. Configuration of
 * every catalog will be saved into a single file. The file name will be {catalogName}.yaml by
 * default.
 */
@Internal
public class FileCatalogStore extends AbstractCatalogStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileCatalogStore.class);

    static final String FILE_EXTENSION = ".yaml";

    /** The YAML mapper to use when reading and writing catalog files. */
    private static final YAMLMapper YAML_MAPPER =
            new YAMLMapper().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);

    /** The directory path where catalog configurations will be stored. */
    private final Path catalogStorePath;

    /**
     * Creates a new {@link FileCatalogStore} instance with the specified directory path.
     *
     * @param catalogStorePath the directory path where catalog configurations will be stored
     */
    public FileCatalogStore(String catalogStorePath) {
        this.catalogStorePath = new Path(catalogStorePath);
    }

    /**
     * Opens the catalog store and initializes the catalog file map.
     *
     * @throws CatalogException if the catalog store directory does not exist, not a directory, or
     *     if there is an error reading the directory
     */
    @Override
    public void open() throws CatalogException {
        try {
            FileSystem fs = catalogStorePath.getFileSystem();
            if (!fs.exists(catalogStorePath)) {
                fs.mkdirs(catalogStorePath);
            }

            if (!fs.getFileStatus(catalogStorePath).isDir()) {
                throw new CatalogException(
                        String.format(
                                "Failed to open catalog store. The given catalog store path %s is not a directory.",
                                catalogStorePath));
            }
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to open file catalog store directory %s.", catalogStorePath),
                    e);
        }
        super.open();
    }

    /**
     * Stores the specified catalog in the catalog store.
     *
     * @param catalogName the name of the catalog
     * @param catalog the catalog descriptor to store
     * @throws CatalogException if the catalog store is not open or if there is an error storing the
     *     catalog
     */
    @Override
    public void storeCatalog(String catalogName, CatalogDescriptor catalog)
            throws CatalogException {
        checkOpenState();

        Path catalogPath = getCatalogPath(catalogName);
        try {
            FileSystem fs = catalogPath.getFileSystem();

            if (fs.exists(catalogPath)) {
                throw new CatalogException(
                        String.format(
                                "Catalog %s's store file %s is already exist.",
                                catalogName, catalogPath));
            }

            try (FSDataOutputStream os = fs.create(catalogPath, WriteMode.NO_OVERWRITE)) {
                YAML_MAPPER.writeValue(os, catalog.getConfiguration().toMap());
            }

            LOG.info("Catalog {}'s configuration saved to file {}", catalogName, catalogPath);
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to store catalog %s's configuration to file %s.",
                            catalogName, catalogPath),
                    e);
        }
    }

    /**
     * Removes the specified catalog from the catalog store.
     *
     * @param catalogName the name of the catalog to remove
     * @param ignoreIfNotExists whether to ignore if the catalog does not exist in the catalog store
     * @throws CatalogException if the catalog store is not open or if there is an error removing
     *     the catalog
     */
    @Override
    public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
            throws CatalogException {
        checkOpenState();
        Path catalogPath = getCatalogPath(catalogName);
        try {
            FileSystem fs = catalogPath.getFileSystem();

            if (fs.exists(catalogPath)) {
                fs.delete(catalogPath, false);
            } else if (!ignoreIfNotExists) {
                throw new CatalogException(
                        String.format(
                                "Catalog %s's store file %s does not exist.",
                                catalogName, catalogPath));
            }
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to remove catalog %s's store file.", catalogName), e);
        }
    }

    /**
     * Returns the catalog descriptor for the specified catalog, if it exists in the catalog store.
     *
     * @param catalogName the name of the catalog to retrieve
     * @return an {@link Optional} containing the catalog descriptor, or an empty {@link Optional}
     *     if the catalog does not exist in the catalog store
     * @throws CatalogException if the catalog store is not open or if there is an error retrieving
     *     the catalog
     */
    @Override
    public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
        checkOpenState();
        Path catalogPath = getCatalogPath(catalogName);
        try {
            FileSystem fs = catalogPath.getFileSystem();

            if (!fs.exists(catalogPath)) {
                return Optional.empty();
            }

            try (FSDataInputStream is = fs.open(catalogPath)) {
                Map<String, String> configMap =
                        YAML_MAPPER.readValue(is, new TypeReference<Map<String, String>>() {});

                CatalogDescriptor catalog =
                        CatalogDescriptor.of(catalogName, Configuration.fromMap(configMap));

                return Optional.of(catalog);
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to load catalog %s's configuration from file.", catalogName),
                    e);
        }
    }

    /**
     * Returns a set of all catalog names in the catalog store.
     *
     * @return a set of all catalog names in the catalog store
     * @throws CatalogException if the catalog store is not open or if there is an error retrieving
     *     the list of catalog names
     */
    @Override
    public Set<String> listCatalogs() throws CatalogException {
        checkOpenState();
        try {
            FileStatus[] statusArr = catalogStorePath.getFileSystem().listStatus(catalogStorePath);

            return Arrays.stream(statusArr)
                    .filter(status -> !status.isDir())
                    .map(FileStatus::getPath)
                    .map(Path::getName)
                    .map(filename -> filename.replace(FILE_EXTENSION, ""))
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to list file catalog store directory %s.", catalogStorePath),
                    e);
        }
    }

    /**
     * Returns whether the specified catalog exists in the catalog store.
     *
     * @param catalogName the name of the catalog to check
     * @return {@code true} if the catalog exists in the catalog store, {@code false} otherwise
     * @throws CatalogException if the catalog store is not open or if there is an error checking
     *     for the catalog
     */
    @Override
    public boolean contains(String catalogName) throws CatalogException {
        checkOpenState();
        Path catalogPath = getCatalogPath(catalogName);
        try {
            return catalogPath.getFileSystem().exists(catalogPath);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check if catalog %s exists in the catalog store.",
                            catalogName),
                    e);
        }
    }

    private Path getCatalogPath(String catalogName) {
        return new Path(catalogStorePath, catalogName + FILE_EXTENSION);
    }
}
