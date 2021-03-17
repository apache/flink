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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.ExpressionResolverBuilder;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A manager for dealing with catalog objects such as tables, views, functions, and types. It
 * encapsulates all available catalogs and stores temporary objects.
 */
@Internal
public final class CatalogManager {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

    // A map between names and catalogs.
    private final Map<String, Catalog> catalogs;

    // Those tables take precedence over corresponding permanent tables, thus they shadow
    // tables coming from catalogs.
    private final Map<ObjectIdentifier, CatalogBaseTable> temporaryTables;

    // The name of the current catalog and database
    private String currentCatalogName;

    private String currentDatabaseName;

    private DefaultSchemaResolver schemaResolver;

    // The name of the built-in catalog
    private final String builtInCatalogName;

    private final DataTypeFactory typeFactory;

    private CatalogManager(
            String defaultCatalogName, Catalog defaultCatalog, DataTypeFactory typeFactory) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(defaultCatalogName),
                "Default catalog name cannot be null or empty");
        checkNotNull(defaultCatalog, "Default catalog cannot be null");

        catalogs = new LinkedHashMap<>();
        catalogs.put(defaultCatalogName, defaultCatalog);
        currentCatalogName = defaultCatalogName;
        currentDatabaseName = defaultCatalog.getDefaultDatabase();

        temporaryTables = new HashMap<>();
        // right now the default catalog is always the built-in one
        builtInCatalogName = defaultCatalogName;

        this.typeFactory = typeFactory;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for a fluent definition of a {@link CatalogManager}. */
    public static final class Builder {

        private @Nullable ClassLoader classLoader;

        private @Nullable ReadableConfig config;

        private @Nullable String defaultCatalogName;

        private @Nullable Catalog defaultCatalog;

        private @Nullable ExecutionConfig executionConfig;

        public Builder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Builder config(ReadableConfig config) {
            this.config = config;
            return this;
        }

        public Builder defaultCatalog(String defaultCatalogName, Catalog defaultCatalog) {
            this.defaultCatalogName = defaultCatalogName;
            this.defaultCatalog = defaultCatalog;
            return this;
        }

        public Builder executionConfig(ExecutionConfig executionConfig) {
            this.executionConfig = executionConfig;
            return this;
        }

        public CatalogManager build() {
            checkNotNull(classLoader, "Class loader cannot be null");
            checkNotNull(config, "Config cannot be null");
            return new CatalogManager(
                    defaultCatalogName,
                    defaultCatalog,
                    new DataTypeFactoryImpl(classLoader, config, executionConfig));
        }
    }

    /**
     * Initializes a {@link SchemaResolver} for {@link Schema} resolution.
     *
     * <p>Currently, the resolver cannot be passed in the constructor because of a chicken-and-egg
     * problem between {@link Planner} and {@link CatalogManager}.
     *
     * @see TableEnvironmentImpl#create(EnvironmentSettings)
     */
    public void initSchemaResolver(
            boolean isStreamingMode, ExpressionResolverBuilder expressionResolverBuilder) {
        this.schemaResolver =
                new DefaultSchemaResolver(
                        isStreamingMode, true, typeFactory, expressionResolverBuilder);
    }

    /**
     * Returns a {@link SchemaResolver} for creating {@link ResolvedSchema} from {@link Schema}
     * context-aware.
     */
    public SchemaResolver getSchemaResolver(boolean supportsMetadata) {
        return schemaResolver.withMetadata(supportsMetadata);
    }

    /** Returns a factory for creating fully resolved data types that can be used for planning. */
    public DataTypeFactory getDataTypeFactory() {
        return typeFactory;
    }

    /**
     * Registers a catalog under the given name. The catalog name must be unique.
     *
     * @param catalogName name under which to register the given catalog
     * @param catalog catalog to register
     * @throws CatalogException if the registration of the catalog under the given name failed
     */
    public void registerCatalog(String catalogName, Catalog catalog) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(catalogName),
                "Catalog name cannot be null or empty.");
        checkNotNull(catalog, "Catalog cannot be null");

        if (catalogs.containsKey(catalogName)) {
            throw new CatalogException(format("Catalog %s already exists.", catalogName));
        }

        catalog.open();
        catalogs.put(catalogName, catalog);
    }

    /**
     * Unregisters a catalog under the given name. The catalog name must be existed.
     *
     * @param catalogName name under which to unregister the given catalog.
     * @param ignoreIfNotExists If false exception will be thrown if the table or database or
     *     catalog to be altered does not exist.
     * @throws CatalogException if the unregistration of the catalog under the given name failed
     */
    public void unregisterCatalog(String catalogName, boolean ignoreIfNotExists) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(catalogName),
                "Catalog name cannot be null or empty.");

        if (catalogs.containsKey(catalogName)) {
            Catalog catalog = catalogs.remove(catalogName);
            catalog.close();
        } else if (!ignoreIfNotExists) {
            throw new CatalogException(format("Catalog %s does not exist.", catalogName));
        }
    }

    /**
     * Gets a catalog by name.
     *
     * @param catalogName name of the catalog to retrieve
     * @return the requested catalog or empty if it does not exist
     */
    public Optional<Catalog> getCatalog(String catalogName) {
        return Optional.ofNullable(catalogs.get(catalogName));
    }

    /**
     * Gets the current catalog that will be used when resolving table path.
     *
     * @return the current catalog
     * @see CatalogManager#qualifyIdentifier(UnresolvedIdentifier)
     */
    public String getCurrentCatalog() {
        return currentCatalogName;
    }

    /**
     * Sets the current catalog name that will be used when resolving table path.
     *
     * @param catalogName catalog name to set as current catalog
     * @throws CatalogNotExistException thrown if the catalog doesn't exist
     * @see CatalogManager#qualifyIdentifier(UnresolvedIdentifier)
     */
    public void setCurrentCatalog(String catalogName) throws CatalogNotExistException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(catalogName),
                "Catalog name cannot be null or empty.");

        Catalog potentialCurrentCatalog = catalogs.get(catalogName);
        if (potentialCurrentCatalog == null) {
            throw new CatalogException(
                    format("A catalog with name [%s] does not exist.", catalogName));
        }

        if (!currentCatalogName.equals(catalogName)) {
            currentCatalogName = catalogName;
            currentDatabaseName = potentialCurrentCatalog.getDefaultDatabase();

            LOG.info(
                    "Set the current default catalog as [{}] and the current default database as [{}].",
                    currentCatalogName,
                    currentDatabaseName);
        }
    }

    /**
     * Gets the current database name that will be used when resolving table path.
     *
     * @return the current database
     * @see CatalogManager#qualifyIdentifier(UnresolvedIdentifier)
     */
    public String getCurrentDatabase() {
        return currentDatabaseName;
    }

    /**
     * Sets the current database name that will be used when resolving a table path. The database
     * has to exist in the current catalog.
     *
     * @param databaseName database name to set as current database name
     * @throws CatalogException thrown if the database doesn't exist in the current catalog
     * @see CatalogManager#qualifyIdentifier(UnresolvedIdentifier)
     * @see CatalogManager#setCurrentCatalog(String)
     */
    public void setCurrentDatabase(String databaseName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "The database name cannot be null or empty.");

        if (!catalogs.get(currentCatalogName).databaseExists(databaseName)) {
            throw new CatalogException(
                    format(
                            "A database with name [%s] does not exist in the catalog: [%s].",
                            databaseName, currentCatalogName));
        }

        if (!currentDatabaseName.equals(databaseName)) {
            currentDatabaseName = databaseName;

            LOG.info(
                    "Set the current default database as [{}] in the current default catalog [{}].",
                    currentDatabaseName,
                    currentCatalogName);
        }
    }

    /**
     * Gets the built-in catalog name. The built-in catalog is used for storing all non-serializable
     * transient meta-objects.
     *
     * @return the built-in catalog name
     */
    public String getBuiltInCatalogName() {
        return builtInCatalogName;
    }

    /**
     * Gets the built-in database name in the built-in catalog. The built-in database is used for
     * storing all non-serializable transient meta-objects.
     *
     * @return the built-in database name
     */
    public String getBuiltInDatabaseName() {
        // The default database of the built-in catalog is also the built-in database.
        return catalogs.get(getBuiltInCatalogName()).getDefaultDatabase();
    }

    /**
     * Result of a lookup for a table through {@link #getTable(ObjectIdentifier)}. It combines the
     * {@link CatalogBaseTable} with additional information such as if the table is a temporary
     * table or comes from the catalog.
     */
    public static class TableLookupResult {

        private final boolean isTemporary;
        private final ResolvedCatalogBaseTable<?> resolvedTable;

        @VisibleForTesting
        public static TableLookupResult temporary(ResolvedCatalogBaseTable<?> resolvedTable) {
            return new TableLookupResult(true, resolvedTable);
        }

        @VisibleForTesting
        public static TableLookupResult permanent(ResolvedCatalogBaseTable<?> resolvedTable) {
            return new TableLookupResult(false, resolvedTable);
        }

        private TableLookupResult(boolean isTemporary, ResolvedCatalogBaseTable<?> resolvedTable) {
            this.isTemporary = isTemporary;
            this.resolvedTable = resolvedTable;
        }

        public boolean isTemporary() {
            return isTemporary;
        }

        /** Returns a fully resolved catalog object. */
        public ResolvedCatalogBaseTable<?> getResolvedTable() {
            return resolvedTable;
        }

        public ResolvedSchema getResolvedSchema() {
            return resolvedTable.getResolvedSchema();
        }

        /** Returns the original metadata object returned by the catalog. */
        public CatalogBaseTable getTable() {
            return resolvedTable.getOrigin();
        }
    }

    /**
     * Retrieves a fully qualified table. If the path is not yet fully qualified use {@link
     * #qualifyIdentifier(UnresolvedIdentifier)} first.
     *
     * @param objectIdentifier full path of the table to retrieve
     * @return table that the path points to.
     */
    public Optional<TableLookupResult> getTable(ObjectIdentifier objectIdentifier) {
        CatalogBaseTable temporaryTable = temporaryTables.get(objectIdentifier);
        if (temporaryTable != null) {
            final ResolvedCatalogBaseTable<?> resolvedTable =
                    resolveCatalogBaseTable(temporaryTable);
            return Optional.of(TableLookupResult.temporary(resolvedTable));
        } else {
            return getPermanentTable(objectIdentifier);
        }
    }

    /**
     * Retrieves a partition with a fully qualified table path and partition spec. If the path is
     * not yet fully qualified use{@link #qualifyIdentifier(UnresolvedIdentifier)} first.
     *
     * @param tableIdentifier full path of the table to retrieve
     * @param partitionSpec full partition spec
     * @return partition in the table.
     */
    public Optional<CatalogPartition> getPartition(
            ObjectIdentifier tableIdentifier, CatalogPartitionSpec partitionSpec) {
        Catalog catalog = catalogs.get(tableIdentifier.getCatalogName());
        if (catalog != null) {
            try {
                return Optional.of(
                        catalog.getPartition(tableIdentifier.toObjectPath(), partitionSpec));
            } catch (PartitionNotExistException ignored) {
            }
        }
        return Optional.empty();
    }

    private Optional<TableLookupResult> getPermanentTable(ObjectIdentifier objectIdentifier) {
        Catalog currentCatalog = catalogs.get(objectIdentifier.getCatalogName());
        ObjectPath objectPath = objectIdentifier.toObjectPath();
        if (currentCatalog != null) {
            try {
                final CatalogBaseTable table = currentCatalog.getTable(objectPath);
                final ResolvedCatalogBaseTable<?> resolvedTable = resolveCatalogBaseTable(table);
                return Optional.of(TableLookupResult.permanent(resolvedTable));
            } catch (TableNotExistException e) {
                // Ignore.
            }
        }
        return Optional.empty();
    }

    private Optional<CatalogBaseTable> getUnresolvedTable(ObjectIdentifier objectIdentifier) {
        Catalog currentCatalog = catalogs.get(objectIdentifier.getCatalogName());
        ObjectPath objectPath = objectIdentifier.toObjectPath();
        if (currentCatalog != null) {
            try {
                final CatalogBaseTable table = currentCatalog.getTable(objectPath);
                return Optional.of(table);
            } catch (TableNotExistException e) {
                // Ignore.
            }
        }
        return Optional.empty();
    }

    /**
     * Retrieves names of all registered catalogs.
     *
     * @return a set of names of registered catalogs
     */
    public Set<String> listCatalogs() {
        return Collections.unmodifiableSet(catalogs.keySet());
    }

    /**
     * Returns an array of names of all tables (tables and views, both temporary and permanent)
     * registered in the namespace of the current catalog and database.
     *
     * @return names of all registered tables
     */
    public Set<String> listTables() {
        return listTables(getCurrentCatalog(), getCurrentDatabase());
    }

    /**
     * Returns an array of names of all tables (tables and views, both temporary and permanent)
     * registered in the namespace of the current catalog and database.
     *
     * @return names of all registered tables
     */
    public Set<String> listTables(String catalogName, String databaseName) {
        Catalog currentCatalog = catalogs.get(getCurrentCatalog());

        try {
            return Stream.concat(
                            currentCatalog.listTables(getCurrentDatabase()).stream(),
                            listTemporaryTablesInternal(catalogName, databaseName)
                                    .map(e -> e.getKey().getObjectName()))
                    .collect(Collectors.toSet());
        } catch (DatabaseNotExistException e) {
            throw new ValidationException("Current database does not exist", e);
        }
    }

    /**
     * Returns an array of names of temporary tables registered in the namespace of the current
     * catalog and database.
     *
     * @return names of registered temporary tables
     */
    public Set<String> listTemporaryTables() {
        return listTemporaryTablesInternal(getCurrentCatalog(), getCurrentDatabase())
                .map(e -> e.getKey().getObjectName())
                .collect(Collectors.toSet());
    }

    /**
     * Returns an array of names of temporary views registered in the namespace of the current
     * catalog and database.
     *
     * @return names of registered temporary views
     */
    public Set<String> listTemporaryViews() {
        return listTemporaryViewsInternal(getCurrentCatalog(), getCurrentDatabase())
                .map(e -> e.getKey().getObjectName())
                .collect(Collectors.toSet());
    }

    private Stream<Map.Entry<ObjectIdentifier, CatalogBaseTable>> listTemporaryTablesInternal(
            String catalogName, String databaseName) {
        return temporaryTables.entrySet().stream()
                .filter(
                        e -> {
                            ObjectIdentifier identifier = e.getKey();
                            return identifier.getCatalogName().equals(catalogName)
                                    && identifier.getDatabaseName().equals(databaseName);
                        });
    }

    /**
     * Returns an array of names of all views(both temporary and permanent) registered in the
     * namespace of the current catalog and database.
     *
     * @return names of all registered views
     */
    public Set<String> listViews() {
        return listViews(getCurrentCatalog(), getCurrentDatabase());
    }

    /**
     * Returns an array of names of all views(both temporary and permanent) registered in the
     * namespace of the current catalog and database.
     *
     * @return names of registered views
     */
    public Set<String> listViews(String catalogName, String databaseName) {
        Catalog currentCatalog = catalogs.get(getCurrentCatalog());

        try {
            return Stream.concat(
                            currentCatalog.listViews(getCurrentDatabase()).stream(),
                            listTemporaryViewsInternal(catalogName, databaseName)
                                    .map(e -> e.getKey().getObjectName()))
                    .collect(Collectors.toSet());
        } catch (DatabaseNotExistException e) {
            throw new ValidationException("Current database does not exist", e);
        }
    }

    private Stream<Map.Entry<ObjectIdentifier, CatalogBaseTable>> listTemporaryViewsInternal(
            String catalogName, String databaseName) {
        return listTemporaryTablesInternal(catalogName, databaseName)
                .filter(e -> e.getValue() instanceof CatalogView);
    }

    /**
     * Lists all available schemas in the root of the catalog manager. It is not equivalent to
     * listing all catalogs as it includes also different catalog parts of the temporary objects.
     *
     * <p><b>NOTE:</b>It is primarily used for interacting with Calcite's schema.
     *
     * @return list of schemas in the root of catalog manager
     */
    public Set<String> listSchemas() {
        return Stream.concat(
                        catalogs.keySet().stream(),
                        temporaryTables.keySet().stream().map(ObjectIdentifier::getCatalogName))
                .collect(Collectors.toSet());
    }

    /**
     * Lists all available schemas in the given catalog. It is not equivalent to listing databases
     * within the given catalog as it includes also different database parts of the temporary
     * objects identifiers.
     *
     * <p><b>NOTE:</b>It is primarily used for interacting with Calcite's schema.
     *
     * @param catalogName filter for the catalog part of the schema
     * @return list of schemas with the given prefix
     */
    public Set<String> listSchemas(String catalogName) {
        return Stream.concat(
                        Optional.ofNullable(catalogs.get(catalogName)).map(Catalog::listDatabases)
                                .orElse(Collections.emptyList()).stream(),
                        temporaryTables.keySet().stream()
                                .filter(i -> i.getCatalogName().equals(catalogName))
                                .map(ObjectIdentifier::getDatabaseName))
                .collect(Collectors.toSet());
    }

    /**
     * Checks if there is a catalog with given name or is there a temporary object registered within
     * a given catalog.
     *
     * <p><b>NOTE:</b>It is primarily used for interacting with Calcite's schema.
     *
     * @param catalogName filter for the catalog part of the schema
     * @return true if a subschema exists
     */
    public boolean schemaExists(String catalogName) {
        return getCatalog(catalogName).isPresent()
                || temporaryTables.keySet().stream()
                        .anyMatch(i -> i.getCatalogName().equals(catalogName));
    }

    /**
     * Checks if there is a database with given name in a given catalog or is there a temporary
     * object registered within a given catalog and database.
     *
     * <p><b>NOTE:</b>It is primarily used for interacting with Calcite's schema.
     *
     * @param catalogName filter for the catalog part of the schema
     * @param databaseName filter for the database part of the schema
     * @return true if a subschema exists
     */
    public boolean schemaExists(String catalogName, String databaseName) {
        return temporaryDatabaseExists(catalogName, databaseName)
                || permanentDatabaseExists(catalogName, databaseName);
    }

    private boolean temporaryDatabaseExists(String catalogName, String databaseName) {
        return temporaryTables.keySet().stream()
                .anyMatch(
                        i ->
                                i.getCatalogName().equals(catalogName)
                                        && i.getDatabaseName().equals(databaseName));
    }

    private boolean permanentDatabaseExists(String catalogName, String databaseName) {
        return getCatalog(catalogName).map(c -> c.databaseExists(databaseName)).orElse(false);
    }

    /**
     * Returns the full name of the given table path, this name may be padded with current
     * catalog/database name based on the {@code identifier's} length.
     *
     * @param identifier an unresolved identifier
     * @return a fully qualified object identifier
     */
    public ObjectIdentifier qualifyIdentifier(UnresolvedIdentifier identifier) {
        return ObjectIdentifier.of(
                identifier.getCatalogName().orElseGet(this::getCurrentCatalog),
                identifier.getDatabaseName().orElseGet(this::getCurrentDatabase),
                identifier.getObjectName());
    }

    /**
     * Creates a table in a given fully qualified path.
     *
     * @param table The table to put in the given path.
     * @param objectIdentifier The fully qualified path where to put the table.
     * @param ignoreIfExists If false exception will be thrown if a table exists in the given path.
     */
    public void createTable(
            CatalogBaseTable table, ObjectIdentifier objectIdentifier, boolean ignoreIfExists) {
        execute(
                (catalog, path) ->
                        catalog.createTable(path, resolveCatalogBaseTable(table), ignoreIfExists),
                objectIdentifier,
                false,
                "CreateTable");
    }

    /**
     * Creates a temporary table in a given fully qualified path.
     *
     * @param table The table to put in the given path.
     * @param objectIdentifier The fully qualified path where to put the table.
     * @param ignoreIfExists if false exception will be thrown if a table exists in the given path.
     */
    public void createTemporaryTable(
            CatalogBaseTable table, ObjectIdentifier objectIdentifier, boolean ignoreIfExists) {
        Optional<TemporaryOperationListener> listener =
                getTemporaryOperationListener(objectIdentifier);
        temporaryTables.compute(
                objectIdentifier,
                (k, v) -> {
                    if (v != null) {
                        if (!ignoreIfExists) {
                            throw new ValidationException(
                                    String.format(
                                            "Temporary table '%s' already exists",
                                            objectIdentifier));
                        }
                        return v;
                    } else {
                        final CatalogBaseTable resolvedTable = resolveCatalogBaseTable(table);
                        if (listener.isPresent()) {
                            return listener.get()
                                    .onCreateTemporaryTable(
                                            objectIdentifier.toObjectPath(), resolvedTable);
                        }
                        return resolvedTable;
                    }
                });
    }

    /**
     * Drop a temporary table in a given fully qualified path.
     *
     * @param objectIdentifier The fully qualified path of the table to drop.
     * @param ignoreIfNotExists If false exception will be thrown if the table to be dropped does
     *     not exist.
     */
    public void dropTemporaryTable(ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
        dropTemporaryTableInternal(
                objectIdentifier, (table) -> table instanceof CatalogTable, ignoreIfNotExists);
    }

    /**
     * Drop a temporary view in a given fully qualified path.
     *
     * @param objectIdentifier The fully qualified path of the view to drop.
     * @param ignoreIfNotExists If false exception will be thrown if the view to be dropped does not
     *     exist.
     */
    public void dropTemporaryView(ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
        dropTemporaryTableInternal(
                objectIdentifier, (table) -> table instanceof CatalogView, ignoreIfNotExists);
    }

    private void dropTemporaryTableInternal(
            ObjectIdentifier objectIdentifier,
            Predicate<CatalogBaseTable> filter,
            boolean ignoreIfNotExists) {
        CatalogBaseTable catalogBaseTable = temporaryTables.get(objectIdentifier);
        if (filter.test(catalogBaseTable)) {
            getTemporaryOperationListener(objectIdentifier)
                    .ifPresent(l -> l.onDropTemporaryTable(objectIdentifier.toObjectPath()));
            temporaryTables.remove(objectIdentifier);
        } else if (!ignoreIfNotExists) {
            throw new ValidationException(
                    String.format(
                            "Temporary table or view with identifier '%s' does not exist.",
                            objectIdentifier.asSummaryString()));
        }
    }

    protected Optional<TemporaryOperationListener> getTemporaryOperationListener(
            ObjectIdentifier identifier) {
        return getCatalog(identifier.getCatalogName())
                .map(
                        c ->
                                c instanceof TemporaryOperationListener
                                        ? (TemporaryOperationListener) c
                                        : null);
    }

    /**
     * Alters a table in a given fully qualified path.
     *
     * @param table The table to put in the given path
     * @param objectIdentifier The fully qualified path where to alter the table.
     * @param ignoreIfNotExists If false exception will be thrown if the table or database or
     *     catalog to be altered does not exist.
     */
    public void alterTable(
            CatalogBaseTable table, ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
        execute(
                (catalog, path) -> {
                    final CatalogBaseTable resolvedTable = resolveCatalogBaseTable(table);
                    catalog.alterTable(path, resolvedTable, ignoreIfNotExists);
                },
                objectIdentifier,
                ignoreIfNotExists,
                "AlterTable");
    }

    /**
     * Drops a table in a given fully qualified path.
     *
     * @param objectIdentifier The fully qualified path of the table to drop.
     * @param ignoreIfNotExists If false exception will be thrown if the table to drop does not
     *     exist.
     */
    public void dropTable(ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
        dropTableInternal(objectIdentifier, ignoreIfNotExists, true);
    }

    /**
     * Drops a view in a given fully qualified path.
     *
     * @param objectIdentifier The fully qualified path of the view to drop.
     * @param ignoreIfNotExists If false exception will be thrown if the view to drop does not
     *     exist.
     */
    public void dropView(ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
        dropTableInternal(objectIdentifier, ignoreIfNotExists, false);
    }

    private void dropTableInternal(
            ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists, boolean isDropTable) {
        Predicate<CatalogBaseTable> filter =
                isDropTable
                        ? table -> table instanceof CatalogTable
                        : table -> table instanceof CatalogView;
        // Same name temporary table or view exists.
        if (filter.test(temporaryTables.get(objectIdentifier))) {
            String tableOrView = isDropTable ? "table" : "view";
            throw new ValidationException(
                    String.format(
                            "Temporary %s with identifier '%s' exists. "
                                    + "Drop it first before removing the permanent %s.",
                            tableOrView, objectIdentifier, tableOrView));
        }
        final Optional<CatalogBaseTable> resultOpt = getUnresolvedTable(objectIdentifier);
        if (resultOpt.isPresent() && filter.test(resultOpt.get())) {
            execute(
                    (catalog, path) -> catalog.dropTable(path, ignoreIfNotExists),
                    objectIdentifier,
                    ignoreIfNotExists,
                    "DropTable");
        } else if (!ignoreIfNotExists) {
            String tableOrView = isDropTable ? "Table" : "View";
            throw new ValidationException(
                    String.format(
                            "%s with identifier '%s' does not exist.",
                            tableOrView, objectIdentifier.asSummaryString()));
        }
    }

    /**
     * A command that modifies given {@link Catalog} in an {@link ObjectPath}. This unifies error
     * handling across different commands.
     */
    private interface ModifyCatalog {
        void execute(Catalog catalog, ObjectPath path) throws Exception;
    }

    private void execute(
            ModifyCatalog command,
            ObjectIdentifier objectIdentifier,
            boolean ignoreNoCatalog,
            String commandName) {
        Optional<Catalog> catalog = getCatalog(objectIdentifier.getCatalogName());
        if (catalog.isPresent()) {
            try {
                command.execute(catalog.get(), objectIdentifier.toObjectPath());
            } catch (TableAlreadyExistException
                    | TableNotExistException
                    | DatabaseNotExistException e) {
                throw new ValidationException(getErrorMessage(objectIdentifier, commandName), e);
            } catch (Exception e) {
                throw new TableException(getErrorMessage(objectIdentifier, commandName), e);
            }
        } else if (!ignoreNoCatalog) {
            throw new ValidationException(
                    String.format("Catalog %s does not exist.", objectIdentifier.getCatalogName()));
        }
    }

    private String getErrorMessage(ObjectIdentifier objectIdentifier, String commandName) {
        return String.format("Could not execute %s in path %s", commandName, objectIdentifier);
    }

    /** Resolves a {@link CatalogBaseTable} to a validated {@link ResolvedCatalogBaseTable}. */
    public ResolvedCatalogBaseTable<?> resolveCatalogBaseTable(CatalogBaseTable baseTable) {
        Preconditions.checkState(schemaResolver != null, "Schema resolver is not initialized.");
        if (baseTable instanceof CatalogTable) {
            return resolveCatalogTable((CatalogTable) baseTable);
        } else if (baseTable instanceof CatalogView) {
            return resolveCatalogView((CatalogView) baseTable);
        }
        throw new IllegalArgumentException(
                "Unknown kind of catalog base table: " + baseTable.getClass());
    }

    private ResolvedCatalogTable resolveCatalogTable(CatalogTable table) {
        if (table instanceof ResolvedCatalogTable) {
            return (ResolvedCatalogTable) table;
        }
        final ResolvedSchema resolvedSchema = table.getUnresolvedSchema().resolve(schemaResolver);
        return new ResolvedCatalogTable(table, resolvedSchema);
    }

    private ResolvedCatalogView resolveCatalogView(CatalogView view) {
        if (view instanceof ResolvedCatalogView) {
            return (ResolvedCatalogView) view;
        }
        final ResolvedSchema resolvedSchema = view.getUnresolvedSchema().resolve(schemaResolver);
        return new ResolvedCatalogView(view, resolvedSchema);
    }
}
