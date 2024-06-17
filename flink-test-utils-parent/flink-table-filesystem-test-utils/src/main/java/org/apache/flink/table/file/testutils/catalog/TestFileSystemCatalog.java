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

package org.apache.flink.table.file.testutils.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.file.testutils.TestFileSystemTableFactory;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.file.testutils.TestFileSystemTableFactory.IDENTIFIER;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A catalog implementation for test {@link FileSystem}. */
public class TestFileSystemCatalog extends AbstractCatalog {

    public static final String SCHEMA_PATH = "schema";
    public static final String DATA_PATH = "data";
    private static final String SCHEMA_FILE_EXTENSION = ".json";

    private final String catalogPathStr;

    private Path catalogPath;
    private FileSystem fs;

    public TestFileSystemCatalog(String pathStr, String name, String defaultDatabase) {
        super(name, defaultDatabase);
        this.catalogPathStr = pathStr;
    }

    @VisibleForTesting
    public String getCatalogPathStr() {
        return catalogPathStr;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new TestFileSystemTableFactory());
    }

    @Override
    public void open() throws CatalogException {
        try {
            catalogPath = new Path(catalogPathStr);
            fs = catalogPath.getFileSystem();
            if (!fs.exists(catalogPath)) {
                throw new CatalogException(
                        String.format(
                                "Catalog %s path %s does not exist.", getName(), catalogPath));
            }
            if (!fs.getFileStatus(catalogPath).isDir()) {
                throw new CatalogException(
                        String.format(
                                "Failed to open catalog path. The given path %s is not a directory.",
                                catalogPath));
            }
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Checking catalog path %s exists occur exception.", catalogPath),
                    e);
        }
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            FileStatus[] fileStatuses = fs.listStatus(catalogPath);
            return Arrays.stream(fileStatuses)
                    .filter(FileStatus::isDir)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("Listing database occur exception.", e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (databaseExists(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "The database name cannot be null or empty.");

        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
        }

        if (!CollectionUtil.isNullOrEmpty(database.getProperties())) {
            throw new CatalogException(
                    "TestFilesystem catalog doesn't support to create database with options.");
        }

        Path dbPath = new Path(catalogPath, name);
        try {
            fs.mkdirs(dbPath);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Creating database %s occur exception.", name), e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(getName(), databaseName);
            }
        }

        List<String> tables = listTables(databaseName);
        if (!tables.isEmpty() && !cascade) {
            throw new DatabaseNotEmptyException(getName(), databaseName);
        }

        if (databaseName.equals(getDefaultDatabase())) {
            throw new IllegalArgumentException(
                    "TestFilesystem catalog doesn't support to drop the default database.");
        }

        Path dbPath = new Path(catalogPath, databaseName);
        try {
            fs.delete(dbPath, true);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Dropping database %s occur exception.", databaseName), e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterDatabase is not implemented.");
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        Path dbPath = new Path(catalogPath, databaseName);
        try {
            return Arrays.stream(fs.listStatus(dbPath))
                    .filter(FileStatus::isDir)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Listing table in database %s occur exception.", dbPath), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        final Path tableSchemaPath =
                tableSchemaFilePath(
                        inferTableSchemaPath(catalogPathStr, tablePath), tablePath.getObjectName());
        final Path tableDataPath = inferTableDataPath(catalogPathStr, tablePath);
        try {
            FileSystemTableInfo tableInfo =
                    JsonSerdeUtil.fromJson(
                            readFileUtf8(tableSchemaPath), FileSystemTableInfo.class);
            return deserializeTable(
                    tableInfo.getTableKind(),
                    tableInfo.getCatalogTableInfo(),
                    tableDataPath.getPath());
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Getting table %s occur exception.", tablePath), e);
        }
    }

    @Internal
    public static boolean isFileSystemTable(Map<String, String> properties) {
        String connector = properties.get(CONNECTOR.key());
        return StringUtils.isNullOrWhitespaceOnly(connector)
                || IDENTIFIER.equalsIgnoreCase(connector);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Path path = inferTablePath(catalogPathStr, tablePath);
        Path tableSchemaFilePath =
                tableSchemaFilePath(
                        inferTableSchemaPath(catalogPathStr, tablePath), tablePath.getObjectName());
        try {
            return fs.exists(path) && fs.exists(tableSchemaFilePath);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Checking table %s exists occur exception.", tablePath), e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(getName(), tablePath);
            }
        }

        Path path = inferTablePath(catalogPathStr, tablePath);
        try {
            fs.delete(path, true);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Dropping table %s occur exception.", tablePath), e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("renameTable is not implemented.");
    }

    @Override
    public void createTable(
            ObjectPath tablePath, CatalogBaseTable catalogTable, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
        }

        if (catalogTable instanceof CatalogView) {
            throw new UnsupportedOperationException(
                    "TestFilesystem catalog doesn't support to CREATE VIEW.");
        }

        Tuple4<Path, Path, Path, String> tableSchemaTuple =
                getTableJsonInfo(tablePath, catalogTable);
        Path path = tableSchemaTuple.f0;
        Path tableSchemaPath = tableSchemaTuple.f1;
        Path tableDataPath = tableSchemaTuple.f2;
        String jsonSchema = tableSchemaTuple.f3;
        try {
            if (!fs.exists(path)) {
                fs.mkdirs(path);
                fs.mkdirs(tableSchemaPath);
                if (isFileSystemTable(catalogTable.getOptions())) {
                    fs.mkdirs(tableDataPath);
                }
            }

            // write table schema
            Path tableSchemaFilePath =
                    tableSchemaFilePath(tableSchemaPath, tablePath.getObjectName());
            try (FSDataOutputStream os =
                    fs.create(tableSchemaFilePath, FileSystem.WriteMode.NO_OVERWRITE)) {
                os.write(jsonSchema.getBytes(StandardCharsets.UTF_8));
            }

        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Create table %s occur exception.", tablePath), e);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterTable is not implemented");
    }

    @Override
    public void alterTable(
            ObjectPath tablePath,
            CatalogBaseTable newTable,
            List<TableChange> tableChanges,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists && !tableExists(tablePath)) {
            return;
        }

        Tuple4<Path, Path, Path, String> tableSchemaInfo = getTableJsonInfo(tablePath, newTable);
        Path tableSchemaPath = tableSchemaInfo.f1;
        String jsonSchema = tableSchemaInfo.f3;
        try {
            if (!fs.exists(tableSchemaPath)) {
                throw new CatalogException(
                        String.format(
                                "Table %s schema file %s doesn't exists.",
                                tablePath, tableSchemaPath));
            }
            // write new table schema
            Path tableSchemaFilePath =
                    tableSchemaFilePath(tableSchemaPath, tablePath.getObjectName());
            try (FSDataOutputStream os =
                    fs.create(tableSchemaFilePath, FileSystem.WriteMode.OVERWRITE)) {
                os.write(jsonSchema.getBytes(StandardCharsets.UTF_8));
            }

        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Altering table %s occur exception.", tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException("createPartition is not implemented.");
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("dropPartition is not implemented.");
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartition is not implemented.");
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("createFunction is not implemented.");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterFunction is not implemented.");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("dropFunction is not implemented.");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterTableStatistics is not implemented.");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException("alterTableColumnStatistics is not implemented.");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartitionStatistics is not implemented.");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "alterPartitionColumnStatistics is not implemented.");
    }

    private Tuple4<Path, Path, Path, String> getTableJsonInfo(
            ObjectPath tablePath, CatalogBaseTable catalogTable) {
        final Path path = inferTablePath(catalogPathStr, tablePath);
        final Path tableSchemaPath = inferTableSchemaPath(catalogPathStr, tablePath);
        final Path tableDataPathStr = inferTableDataPath(catalogPathStr, tablePath);

        ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable =
                (ResolvedCatalogBaseTable<?>) catalogTable;
        Map<String, String> catalogTableInfo = serializeTable(resolvedCatalogBaseTable);
        CatalogBaseTable.TableKind tableKind = catalogTable.getTableKind();
        FileSystemTableInfo tableInfo = new FileSystemTableInfo(tableKind, catalogTableInfo);

        String jsonSchema = JsonSerdeUtil.toJson(tableInfo);
        return Tuple4.of(path, tableSchemaPath, tableDataPathStr, jsonSchema);
    }

    /** Read file to UTF_8 decoding. */
    private String readFileUtf8(Path path) throws IOException {
        try (FSDataInputStream in = path.getFileSystem().open(path)) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        }
    }

    private Path inferTablePath(String catalogPath, ObjectPath tablePath) {
        return new Path(
                String.format(
                        "%s/%s/%s",
                        catalogPath, tablePath.getDatabaseName(), tablePath.getObjectName()));
    }

    private Path inferTableDataPath(String catalogPath, ObjectPath tablePath) {
        return new Path(
                String.format(
                        "%s/%s/%s/%s",
                        catalogPath,
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName(),
                        DATA_PATH));
    }

    private Path inferTableSchemaPath(String catalogPath, ObjectPath tablePath) {
        return new Path(
                String.format(
                        "%s/%s/%s/%s",
                        catalogPath,
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName(),
                        SCHEMA_PATH));
    }

    private Path tableSchemaFilePath(Path tableSchemaPath, String tableName) {
        return new Path(tableSchemaPath, tableName + "_schema" + SCHEMA_FILE_EXTENSION);
    }

    private Map<String, String> serializeTable(
            ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable) {
        if (resolvedCatalogBaseTable instanceof ResolvedCatalogTable) {
            return CatalogPropertiesUtil.serializeCatalogTable(
                    (ResolvedCatalogTable) resolvedCatalogBaseTable);
        } else if (resolvedCatalogBaseTable instanceof ResolvedCatalogMaterializedTable) {
            return CatalogPropertiesUtil.serializeCatalogMaterializedTable(
                    (ResolvedCatalogMaterializedTable) resolvedCatalogBaseTable);
        }

        throw new IllegalArgumentException(
                "Unknown kind of resolved catalog base table: "
                        + resolvedCatalogBaseTable.getClass());
    }

    private CatalogBaseTable deserializeTable(
            CatalogBaseTable.TableKind tableKind,
            Map<String, String> properties,
            String tableDataPath) {
        if (CatalogBaseTable.TableKind.TABLE == tableKind) {
            CatalogTable catalogTable = CatalogPropertiesUtil.deserializeCatalogTable(properties);
            Map<String, String> options = new HashMap<>(catalogTable.getOptions());
            if (isFileSystemTable(catalogTable.getOptions())) {
                // put table data path
                options.put(FileSystemConnectorOptions.PATH.key(), tableDataPath);
            }
            return catalogTable.copy(options);
        } else if (CatalogBaseTable.TableKind.MATERIALIZED_TABLE == tableKind) {
            CatalogMaterializedTable catalogMaterializedTable =
                    CatalogPropertiesUtil.deserializeCatalogMaterializedTable(properties);
            Map<String, String> options = new HashMap<>(catalogMaterializedTable.getOptions());
            if (isFileSystemTable(catalogMaterializedTable.getOptions())) {
                // put table data path
                options.put(FileSystemConnectorOptions.PATH.key(), tableDataPath);
            }
            return catalogMaterializedTable.copy(options);
        }

        throw new IllegalArgumentException("Unknown catalog base table kind: " + tableKind);
    }

    /** The pojo class represents serializable catalog base table info. */
    public static class FileSystemTableInfo {

        private final CatalogBaseTable.TableKind tableKind;
        private final Map<String, String> catalogTableInfo;

        @JsonCreator
        public FileSystemTableInfo(
                @JsonProperty("tableKind") CatalogBaseTable.TableKind tableKind,
                @JsonProperty("catalogTableInfo") Map<String, String> catalogTableInfo) {
            this.tableKind = tableKind;
            this.catalogTableInfo = catalogTableInfo;
        }

        public CatalogBaseTable.TableKind getTableKind() {
            return tableKind;
        }

        public Map<String, String> getCatalogTableInfo() {
            return catalogTableInfo;
        }
    }
}
