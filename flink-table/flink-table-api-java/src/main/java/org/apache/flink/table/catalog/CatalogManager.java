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
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.listener.AlterDatabaseEvent;
import org.apache.flink.table.catalog.listener.AlterTableEvent;
import org.apache.flink.table.catalog.listener.CatalogContext;
import org.apache.flink.table.catalog.listener.CatalogModificationListener;
import org.apache.flink.table.catalog.listener.CreateDatabaseEvent;
import org.apache.flink.table.catalog.listener.CreateTableEvent;
import org.apache.flink.table.catalog.listener.DropDatabaseEvent;
import org.apache.flink.table.catalog.listener.DropTableEvent;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.ExpressionResolverBuilder;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
public final class CatalogManager implements CatalogRegistry, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

    // A map between names and catalogs.
    private final Map<String, Catalog> catalogs;

    // Those tables take precedence over corresponding permanent tables, thus they shadow
    // tables coming from catalogs.
    private final Map<ObjectIdentifier, CatalogBaseTable> temporaryTables;

    // The name of the current catalog and database
    private @Nullable String currentCatalogName;

    private @Nullable String currentDatabaseName;

    private DefaultSchemaResolver schemaResolver;

    // The name of the built-in catalog
    private final String builtInCatalogName;

    private final DataTypeFactory typeFactory;

    private final ManagedTableListener managedTableListener;

    private final List<CatalogModificationListener> catalogModificationListeners;

    private final CatalogStoreHolder catalogStoreHolder;

    private CatalogManager(
            String defaultCatalogName,
            Catalog defaultCatalog,
            DataTypeFactory typeFactory,
            ManagedTableListener managedTableListener,
            List<CatalogModificationListener> catalogModificationListeners,
            CatalogStoreHolder catalogStoreHolder) {
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
        this.managedTableListener = managedTableListener;
        this.catalogModificationListeners = catalogModificationListeners;

        this.catalogStoreHolder = catalogStoreHolder;
    }

    @VisibleForTesting
    public List<CatalogModificationListener> getCatalogModificationListeners() {
        return catalogModificationListeners;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for a fluent definition of a {@link CatalogManager}. */
    @Internal
    public static final class Builder {

        private @Nullable ClassLoader classLoader;

        private @Nullable ReadableConfig config;

        private @Nullable String defaultCatalogName;

        private @Nullable Catalog defaultCatalog;

        private @Nullable ExecutionConfig executionConfig;

        private @Nullable DataTypeFactory dataTypeFactory;

        private List<CatalogModificationListener> catalogModificationListeners =
                Collections.emptyList();
        private CatalogStoreHolder catalogStoreHolder;

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

        public Builder dataTypeFactory(DataTypeFactory dataTypeFactory) {
            this.dataTypeFactory = dataTypeFactory;
            return this;
        }

        public Builder catalogModificationListeners(
                List<CatalogModificationListener> catalogModificationListeners) {
            this.catalogModificationListeners = catalogModificationListeners;
            return this;
        }

        public Builder catalogStoreHolder(CatalogStoreHolder catalogStoreHolder) {
            this.catalogStoreHolder = catalogStoreHolder;
            return this;
        }

        public CatalogManager build() {
            checkNotNull(classLoader, "Class loader cannot be null");
            checkNotNull(config, "Config cannot be null");
            checkNotNull(catalogStoreHolder, "CatalogStoreHolder cannot be null");
            return new CatalogManager(
                    defaultCatalogName,
                    defaultCatalog,
                    dataTypeFactory != null
                            ? dataTypeFactory
                            : new DataTypeFactoryImpl(classLoader, config, executionConfig),
                    new ManagedTableListener(classLoader, config),
                    catalogModificationListeners,
                    catalogStoreHolder);
        }
    }

    /**
     * Closes the catalog manager and releases its resources.
     *
     * <p>This method closes all initialized catalogs and the catalog store.
     *
     * @throws CatalogException if an error occurs while closing the catalogs or the catalog store
     */
    public void close() throws CatalogException {
        // close the initialized catalogs
        List<Throwable> errors = new ArrayList<>();
        for (Map.Entry<String, Catalog> entry : catalogs.entrySet()) {
            String catalogName = entry.getKey();
            Catalog catalog = entry.getValue();
            try {
                catalog.close();
            } catch (Throwable e) {
                LOG.error(
                        String.format(
                                "Failed to close catalog %s: %s", catalogName, e.getMessage()),
                        e);
                errors.add(e);
            }
        }

        // close the catalog store holder
        try {
            catalogStoreHolder.close();
        } catch (Throwable e) {
            errors.add(e);
            LOG.error(String.format("Failed to close catalog store holder: %s", e.getMessage()), e);
        }

        if (!errors.isEmpty()) {
            CatalogException exception = new CatalogException("Failed to close catalog manager");
            for (Throwable e : errors) {
                exception.addSuppressed(e);
            }
            throw exception;
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
                new DefaultSchemaResolver(isStreamingMode, typeFactory, expressionResolverBuilder);
    }

    /** Returns a {@link SchemaResolver} for creating {@link ResolvedSchema} from {@link Schema}. */
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    /** Returns a factory for creating fully resolved data types that can be used for planning. */
    public DataTypeFactory getDataTypeFactory() {
        return typeFactory;
    }

    /**
     * Creates a catalog under the given name. The catalog name must be unique.
     *
     * @param catalogName the given catalog name under which to create the given catalog
     * @param catalogDescriptor catalog descriptor for creating catalog
     * @throws CatalogException If the catalog already exists in the catalog store or initialized
     *     catalogs, or if an error occurs while creating the catalog or storing the {@link
     *     CatalogDescriptor}
     */
    public void createCatalog(String catalogName, CatalogDescriptor catalogDescriptor)
            throws CatalogException {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(catalogName),
                "Catalog name cannot be null or empty.");
        checkNotNull(catalogDescriptor, "Catalog descriptor cannot be null");

        if (catalogStoreHolder.catalogStore().contains(catalogName)) {
            throw new CatalogException(
                    format("Catalog %s already exists in catalog store.", catalogName));
        }
        if (catalogs.containsKey(catalogName)) {
            throw new CatalogException(
                    format("Catalog %s already exists in initialized catalogs.", catalogName));
        }

        Catalog catalog = initCatalog(catalogName, catalogDescriptor);
        catalog.open();
        catalogs.put(catalogName, catalog);

        catalogStoreHolder.catalogStore().storeCatalog(catalogName, catalogDescriptor);
    }

    private Catalog initCatalog(String catalogName, CatalogDescriptor catalogDescriptor) {
        return FactoryUtil.createCatalog(
                catalogName,
                catalogDescriptor.getConfiguration().toMap(),
                catalogStoreHolder.config(),
                catalogStoreHolder.classLoader());
    }

    /**
     * Registers a catalog under the given name. The catalog name must be unique.
     *
     * @param catalogName name under which to register the given catalog
     * @param catalog catalog to register
     * @throws CatalogException if the registration of the catalog under the given name failed
     * @deprecated This method is deprecated and will be removed in a future release. Use {@code
     *     createCatalog} instead to create a catalog using {@link CatalogDescriptor} and store it
     *     in the {@link CatalogStore}.
     */
    @Deprecated
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
     * <p>If the catalog is in the initialized catalogs, it will be removed from the initialized
     * catalogs. If the catalog is stored in the {@link CatalogStore}, it will be removed from the
     * CatalogStore.
     *
     * @param catalogName name under which to unregister the given catalog.
     * @param ignoreIfNotExists If false exception will be thrown if the table or database or
     *     catalog to be altered does not exist.
     * @throws CatalogException If the catalog does not exist in the initialized catalogs and not in
     *     the {@link CatalogStore}, or if the remove operation failed.
     */
    public void unregisterCatalog(String catalogName, boolean ignoreIfNotExists) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(catalogName),
                "Catalog name cannot be null or empty.");

        if (catalogs.containsKey(catalogName)
                || catalogStoreHolder.catalogStore().contains(catalogName)) {
            if (catalogName.equals(currentCatalogName)) {
                throw new CatalogException("Cannot drop a catalog which is currently in use.");
            }
            if (catalogs.containsKey(catalogName)) {
                Catalog catalog = catalogs.remove(catalogName);
                catalog.close();
            }
            if (catalogStoreHolder.catalogStore().contains(catalogName)) {
                catalogStoreHolder.catalogStore().removeCatalog(catalogName, ignoreIfNotExists);
            }
        } else if (!ignoreIfNotExists) {
            throw new CatalogException(format("Catalog %s does not exist.", catalogName));
        }
    }

    /**
     * Gets a {@link Catalog} instance by name.
     *
     * <p>If the catalog has already been initialized, the initialized instance will be returned
     * directly. Otherwise, the {@link CatalogDescriptor} will be obtained from the {@link
     * CatalogStore}, and the catalog instance will be initialized.
     *
     * @param catalogName name of the catalog to retrieve
     * @return the requested catalog or empty if it does not exist
     */
    public Optional<Catalog> getCatalog(String catalogName) {
        // Get catalog from the initialized catalogs.
        if (catalogs.containsKey(catalogName)) {
            return Optional.of(catalogs.get(catalogName));
        }

        // Get catalog from the CatalogStore.
        Optional<CatalogDescriptor> optionalDescriptor =
                catalogStoreHolder.catalogStore().getCatalog(catalogName);
        return optionalDescriptor.map(
                descriptor -> {
                    Catalog catalog = initCatalog(catalogName, descriptor);
                    catalog.open();
                    catalogs.put(catalogName, catalog);
                    return catalog;
                });
    }

    public Catalog getCatalogOrThrowException(String catalogName) {
        return getCatalog(catalogName)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format("Catalog %s does not exist", catalogName)));
    }

    /**
     * Gets a catalog by name.
     *
     * @param catalogName name of the catalog to retrieve
     * @return the requested catalog
     * @throws CatalogNotExistException if the catalog does not exist
     */
    public Catalog getCatalogOrError(String catalogName) throws CatalogNotExistException {
        return getCatalog(catalogName).orElseThrow(() -> new CatalogNotExistException(catalogName));
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
    public void setCurrentCatalog(@Nullable String catalogName) throws CatalogNotExistException {
        if (catalogName == null) {
            this.currentCatalogName = null;
            this.currentDatabaseName = null;
            return;
        }

        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(catalogName), "Catalog name cannot be empty.");

        Catalog potentialCurrentCatalog =
                getCatalog(catalogName)
                        .orElseThrow(
                                () ->
                                        new CatalogException(
                                                format(
                                                        "A catalog with name [%s] does not exist.",
                                                        catalogName)));
        if (!catalogName.equals(currentCatalogName)) {
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
    public void setCurrentDatabase(@Nullable String databaseName) {
        if (databaseName == null) {
            this.currentDatabaseName = null;
            return;
        }

        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "The database name cannot be empty.");

        if (currentCatalogName == null) {
            throw new CatalogException("Current catalog has not been set.");
        }

        if (!getCatalogOrThrowException(currentCatalogName).databaseExists(databaseName)) {
            throw new CatalogException(
                    format(
                            "A database with name [%s] does not exist in the catalog: [%s].",
                            databaseName, currentCatalogName));
        }

        if (!databaseName.equals(currentDatabaseName)) {
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
        return getCatalogOrThrowException(getBuiltInCatalogName()).getDefaultDatabase();
    }

    /**
     * Retrieves a fully qualified table. If the path is not yet fully qualified use {@link
     * #qualifyIdentifier(UnresolvedIdentifier)} first.
     *
     * @param objectIdentifier full path of the table to retrieve
     * @return table that the path points to.
     */
    public Optional<ContextResolvedTable> getTable(ObjectIdentifier objectIdentifier) {
        CatalogBaseTable temporaryTable = temporaryTables.get(objectIdentifier);
        if (temporaryTable != null) {
            final ResolvedCatalogBaseTable<?> resolvedTable =
                    resolveCatalogBaseTable(temporaryTable);
            return Optional.of(ContextResolvedTable.temporary(objectIdentifier, resolvedTable));
        } else {
            return getPermanentTable(objectIdentifier, null);
        }
    }

    /**
     * Retrieves a fully qualified table with a specific time. If the path is not yet fully
     * qualified, use {@link #qualifyIdentifier(UnresolvedIdentifier)} first.
     *
     * @param objectIdentifier full path of the table to retrieve
     * @param timestamp Timestamp of the table snapshot, which is milliseconds since 1970-01-01
     *     00:00:00 UTC
     * @return table at a specific time that the path points to.
     */
    public Optional<ContextResolvedTable> getTable(
            ObjectIdentifier objectIdentifier, long timestamp) {
        CatalogBaseTable temporaryTable = temporaryTables.get(objectIdentifier);
        if (temporaryTable != null) {
            final ResolvedCatalogBaseTable<?> resolvedTable =
                    resolveCatalogBaseTable(temporaryTable);
            return Optional.of(ContextResolvedTable.temporary(objectIdentifier, resolvedTable));
        } else {
            return getPermanentTable(objectIdentifier, timestamp);
        }
    }

    /**
     * Retrieves a fully qualified table. If the path is not yet fully qualified use {@link
     * #qualifyIdentifier(UnresolvedIdentifier)} first.
     *
     * @param objectIdentifier full path of the table to retrieve
     * @return resolved table that the path points to or empty if it does not exist.
     */
    @Override
    public Optional<ResolvedCatalogBaseTable<?>> getCatalogBaseTable(
            ObjectIdentifier objectIdentifier) {
        ContextResolvedTable resolvedTable = getTable(objectIdentifier).orElse(null);
        return resolvedTable == null
                ? Optional.empty()
                : Optional.of(resolvedTable.getResolvedTable());
    }

    /**
     * Return whether the table with a fully qualified table path is temporary or not.
     *
     * @param objectIdentifier full path of the table
     * @return the table is temporary or not.
     */
    @Override
    public boolean isTemporaryTable(ObjectIdentifier objectIdentifier) {
        return temporaryTables.containsKey(objectIdentifier);
    }

    /**
     * Like {@link #getTable(ObjectIdentifier)}, but throws an error when the table is not available
     * in any of the catalogs.
     */
    public ContextResolvedTable getTableOrError(ObjectIdentifier objectIdentifier) {
        return getTable(objectIdentifier)
                .orElseThrow(
                        () ->
                                new TableException(
                                        String.format(
                                                "Cannot find table '%s' in any of the catalogs %s, nor as a temporary table.",
                                                objectIdentifier, listCatalogs())));
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
        Optional<Catalog> catalogOptional = getCatalog(tableIdentifier.getCatalogName());
        if (catalogOptional.isPresent()) {
            try {
                return Optional.of(
                        catalogOptional
                                .get()
                                .getPartition(tableIdentifier.toObjectPath(), partitionSpec));
            } catch (PartitionNotExistException ignored) {
            }
        }
        return Optional.empty();
    }

    private Optional<ContextResolvedTable> getPermanentTable(
            ObjectIdentifier objectIdentifier, @Nullable Long timestamp) {
        Optional<Catalog> catalogOptional = getCatalog(objectIdentifier.getCatalogName());
        ObjectPath objectPath = objectIdentifier.toObjectPath();
        if (catalogOptional.isPresent()) {
            Catalog currentCatalog = catalogOptional.get();
            try {
                final CatalogBaseTable table;
                if (timestamp != null) {
                    table = currentCatalog.getTable(objectPath, timestamp);
                    if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
                        throw new TableException(
                                String.format(
                                        "%s is a view, but time travel is not supported for view.",
                                        objectIdentifier.asSummaryString()));
                    }
                } else {
                    table = currentCatalog.getTable(objectPath);
                }
                final ResolvedCatalogBaseTable<?> resolvedTable = resolveCatalogBaseTable(table);
                return Optional.of(
                        ContextResolvedTable.permanent(
                                objectIdentifier, currentCatalog, resolvedTable));
            } catch (TableNotExistException e) {
                // Ignore.
            }
        }
        return Optional.empty();
    }

    private Optional<CatalogBaseTable> getUnresolvedTable(ObjectIdentifier objectIdentifier) {
        Optional<Catalog> currentCatalog = getCatalog(objectIdentifier.getCatalogName());
        ObjectPath objectPath = objectIdentifier.toObjectPath();
        if (currentCatalog.isPresent()) {
            try {
                final CatalogBaseTable table = currentCatalog.get().getTable(objectPath);
                return Optional.of(table);
            } catch (TableNotExistException e) {
                // Ignore.
            }
        }
        return Optional.empty();
    }

    /**
     * Retrieves the set of names of all registered catalogs, including all initialized catalogs and
     * all catalogs stored in the {@link CatalogStore}.
     *
     * @return a set of names of registered catalogs
     */
    public Set<String> listCatalogs() {
        return Collections.unmodifiableSet(
                Stream.concat(
                                catalogs.keySet().stream(),
                                catalogStoreHolder.catalogStore().listCatalogs().stream())
                        .collect(Collectors.toSet()));
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
     * registered in the namespace of the given catalog and database.
     *
     * @return names of all registered tables
     */
    public Set<String> listTables(String catalogName, String databaseName) {
        Catalog catalog = getCatalogOrThrowException(catalogName);
        if (catalog == null) {
            throw new ValidationException(String.format("Catalog %s does not exist", catalogName));
        }

        try {
            return Stream.concat(
                            catalog.listTables(databaseName).stream(),
                            listTemporaryTablesInternal(catalogName, databaseName)
                                    .map(e -> e.getKey().getObjectName()))
                    .collect(Collectors.toSet());
        } catch (DatabaseNotExistException e) {
            throw new ValidationException(
                    String.format("Database %s does not exist", databaseName), e);
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
     * namespace of the given catalog and database.
     *
     * @return names of registered views
     */
    public Set<String> listViews(String catalogName, String databaseName) {
        Catalog catalog = getCatalogOrThrowException(catalogName);
        if (catalog == null) {
            throw new ValidationException(String.format("Catalog %s does not exist", catalogName));
        }

        try {
            return Stream.concat(
                            catalog.listViews(databaseName).stream(),
                            listTemporaryViewsInternal(catalogName, databaseName)
                                    .map(e -> e.getKey().getObjectName()))
                    .collect(Collectors.toSet());
        } catch (DatabaseNotExistException e) {
            throw new ValidationException(
                    String.format("Database %s does not exist", databaseName), e);
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
                        getCatalog(catalogName).map(Catalog::listDatabases)
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
                identifier
                        .getCatalogName()
                        .orElseGet(
                                () -> {
                                    final String currentCatalog = getCurrentCatalog();
                                    if (StringUtils.isNullOrWhitespaceOnly(currentCatalog)) {
                                        throw new ValidationException(
                                                "A current catalog has not been set. Please use a"
                                                        + " fully qualified identifier (such as"
                                                        + " 'my_catalog.my_database.my_table') or"
                                                        + " set a current catalog using"
                                                        + " 'USE CATALOG my_catalog'.");
                                    }
                                    return currentCatalog;
                                }),
                identifier
                        .getDatabaseName()
                        .orElseGet(
                                () -> {
                                    final String currentDatabase = getCurrentDatabase();
                                    if (StringUtils.isNullOrWhitespaceOnly(currentDatabase)) {
                                        throw new ValidationException(
                                                "A current database has not been set. Please use a"
                                                        + " fully qualified identifier (such as"
                                                        + " 'my_database.my_table' or"
                                                        + " 'my_catalog.my_database.my_table') or"
                                                        + " set a current database using"
                                                        + " 'USE my_database'.");
                                    }
                                    return currentDatabase;
                                }),
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
                (catalog, path) -> {
                    ResolvedCatalogBaseTable<?> resolvedTable = resolveCatalogBaseTable(table);
                    ResolvedCatalogBaseTable<?> resolvedListenedTable =
                            managedTableListener.notifyTableCreation(
                                    catalog,
                                    objectIdentifier,
                                    resolvedTable,
                                    false,
                                    ignoreIfExists);

                    catalog.createTable(path, resolvedListenedTable, ignoreIfExists);
                    if (resolvedListenedTable instanceof CatalogTable) {
                        catalogModificationListeners.forEach(
                                listener ->
                                        listener.onEvent(
                                                CreateTableEvent.createEvent(
                                                        CatalogContext.createContext(
                                                                objectIdentifier.getCatalogName(),
                                                                catalog),
                                                        objectIdentifier,
                                                        resolvedListenedTable,
                                                        ignoreIfExists,
                                                        false)));
                    }
                },
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
                        ResolvedCatalogBaseTable<?> resolvedTable = resolveCatalogBaseTable(table);
                        Catalog catalog =
                                getCatalog(objectIdentifier.getCatalogName()).orElse(null);
                        ResolvedCatalogBaseTable<?> resolvedListenedTable =
                                managedTableListener.notifyTableCreation(
                                        catalog,
                                        objectIdentifier,
                                        resolvedTable,
                                        true,
                                        ignoreIfExists);

                        if (listener.isPresent()) {
                            return listener.get()
                                    .onCreateTemporaryTable(
                                            objectIdentifier.toObjectPath(), resolvedListenedTable);
                        }
                        if (resolvedListenedTable instanceof CatalogTable) {
                            catalogModificationListeners.forEach(
                                    l ->
                                            l.onEvent(
                                                    CreateTableEvent.createEvent(
                                                            CatalogContext.createContext(
                                                                    objectIdentifier
                                                                            .getCatalogName(),
                                                                    catalog),
                                                            objectIdentifier,
                                                            resolvedListenedTable,
                                                            ignoreIfExists,
                                                            true)));
                        }
                        return resolvedListenedTable;
                    }
                });
    }

    /**
     * Resolve dynamic options for compact operation on a Flink's managed table.
     *
     * @param origin The resolved managed table with enriched options.
     * @param tableIdentifier The fully qualified path of the managed table.
     * @param partitionSpec User-specified unresolved partition spec.
     * @return dynamic options which describe the metadata of compaction
     */
    public Map<String, String> resolveCompactManagedTableOptions(
            ResolvedCatalogTable origin,
            ObjectIdentifier tableIdentifier,
            CatalogPartitionSpec partitionSpec) {
        return managedTableListener.notifyTableCompaction(
                catalogs.getOrDefault(tableIdentifier.getCatalogName(), null),
                tableIdentifier,
                origin,
                partitionSpec,
                false);
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
                objectIdentifier,
                (table) -> table instanceof CatalogTable,
                ignoreIfNotExists,
                true);
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
                objectIdentifier,
                (table) -> table instanceof CatalogView,
                ignoreIfNotExists,
                false);
    }

    private void dropTemporaryTableInternal(
            ObjectIdentifier objectIdentifier,
            Predicate<CatalogBaseTable> filter,
            boolean ignoreIfNotExists,
            boolean isDropTable) {
        CatalogBaseTable catalogBaseTable = temporaryTables.get(objectIdentifier);
        if (filter.test(catalogBaseTable)) {
            getTemporaryOperationListener(objectIdentifier)
                    .ifPresent(l -> l.onDropTemporaryTable(objectIdentifier.toObjectPath()));

            Catalog catalog = getCatalog(objectIdentifier.getCatalogName()).orElse(null);
            ResolvedCatalogBaseTable<?> resolvedTable = resolveCatalogBaseTable(catalogBaseTable);
            managedTableListener.notifyTableDrop(
                    catalog, objectIdentifier, resolvedTable, true, ignoreIfNotExists);

            temporaryTables.remove(objectIdentifier);
            if (isDropTable) {
                catalogModificationListeners.forEach(
                        listener ->
                                listener.onEvent(
                                        DropTableEvent.createEvent(
                                                CatalogContext.createContext(
                                                        objectIdentifier.getCatalogName(), catalog),
                                                objectIdentifier,
                                                resolvedTable,
                                                ignoreIfNotExists,
                                                true)));
            }
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
                    if (resolvedTable instanceof CatalogTable) {
                        catalogModificationListeners.forEach(
                                listener ->
                                        listener.onEvent(
                                                AlterTableEvent.createEvent(
                                                        CatalogContext.createContext(
                                                                objectIdentifier.getCatalogName(),
                                                                catalog),
                                                        objectIdentifier,
                                                        resolvedTable,
                                                        ignoreIfNotExists)));
                    }
                },
                objectIdentifier,
                ignoreIfNotExists,
                "AlterTable");
    }

    /**
     * Alters a table in a given fully qualified path with table changes.
     *
     * @param table The table to put in the given path
     * @param changes The table changes from the original table to the new table.
     * @param objectIdentifier The fully qualified path where to alter the table.
     * @param ignoreIfNotExists If false exception will be thrown if the table or database or
     *     catalog to be altered does not exist.
     */
    public void alterTable(
            CatalogBaseTable table,
            List<TableChange> changes,
            ObjectIdentifier objectIdentifier,
            boolean ignoreIfNotExists) {
        execute(
                (catalog, path) -> {
                    final CatalogBaseTable resolvedTable = resolveCatalogBaseTable(table);
                    catalog.alterTable(path, resolvedTable, changes, ignoreIfNotExists);
                    if (resolvedTable instanceof CatalogTable) {
                        catalogModificationListeners.forEach(
                                listener ->
                                        listener.onEvent(
                                                AlterTableEvent.createEvent(
                                                        CatalogContext.createContext(
                                                                objectIdentifier.getCatalogName(),
                                                                catalog),
                                                        objectIdentifier,
                                                        resolvedTable,
                                                        ignoreIfNotExists)));
                    }
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
                    (catalog, path) -> {
                        ResolvedCatalogBaseTable<?> resolvedTable =
                                resolveCatalogBaseTable(resultOpt.get());
                        managedTableListener.notifyTableDrop(
                                catalog, objectIdentifier, resolvedTable, false, ignoreIfNotExists);

                        catalog.dropTable(path, ignoreIfNotExists);
                        if (isDropTable) {
                            catalogModificationListeners.forEach(
                                    listener ->
                                            listener.onEvent(
                                                    DropTableEvent.createEvent(
                                                            CatalogContext.createContext(
                                                                    objectIdentifier
                                                                            .getCatalogName(),
                                                                    catalog),
                                                            objectIdentifier,
                                                            resolvedTable,
                                                            ignoreIfNotExists,
                                                            false)));
                        }
                    },
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
        Preconditions.checkNotNull(schemaResolver, "Schema resolver is not initialized.");
        if (baseTable instanceof CatalogTable) {
            return resolveCatalogTable((CatalogTable) baseTable);
        } else if (baseTable instanceof CatalogView) {
            return resolveCatalogView((CatalogView) baseTable);
        }
        throw new IllegalArgumentException(
                "Unknown kind of catalog base table: " + baseTable.getClass());
    }

    /** Resolves a {@link CatalogTable} to a validated {@link ResolvedCatalogTable}. */
    public ResolvedCatalogTable resolveCatalogTable(CatalogTable table) {
        Preconditions.checkNotNull(schemaResolver, "Schema resolver is not initialized.");
        if (table instanceof ResolvedCatalogTable) {
            return (ResolvedCatalogTable) table;
        }

        final ResolvedSchema resolvedSchema = table.getUnresolvedSchema().resolve(schemaResolver);

        // Validate partition keys are included in physical columns
        final List<String> physicalColumns =
                resolvedSchema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .collect(Collectors.toList());
        table.getPartitionKeys()
                .forEach(
                        partitionKey -> {
                            if (!physicalColumns.contains(partitionKey)) {
                                throw new ValidationException(
                                        String.format(
                                                "Invalid partition key '%s'. A partition key must "
                                                        + "reference a physical column in the schema. "
                                                        + "Available columns are: %s",
                                                partitionKey, physicalColumns));
                            }
                        });

        return new ResolvedCatalogTable(table, resolvedSchema);
    }

    /** Resolves a {@link CatalogView} to a validated {@link ResolvedCatalogView}. */
    public ResolvedCatalogView resolveCatalogView(CatalogView view) {
        Preconditions.checkNotNull(schemaResolver, "Schema resolver is not initialized.");
        if (view instanceof ResolvedCatalogView) {
            return (ResolvedCatalogView) view;
        }
        final ResolvedSchema resolvedSchema = view.getUnresolvedSchema().resolve(schemaResolver);
        return new ResolvedCatalogView(view, resolvedSchema);
    }

    /**
     * Create a database.
     *
     * @param catalogName Name of the catalog for database
     * @param databaseName Name of the database to be created
     * @param database The database definition
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *     is false
     * @throws CatalogException in case of any runtime exception
     */
    public void createDatabase(
            String catalogName,
            String databaseName,
            CatalogDatabase database,
            boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        Catalog catalog = getCatalogOrThrowException(catalogName);
        catalog.createDatabase(databaseName, database, ignoreIfExists);
        catalogModificationListeners.forEach(
                listener ->
                        listener.onEvent(
                                CreateDatabaseEvent.createEvent(
                                        CatalogContext.createContext(catalogName, catalog),
                                        databaseName,
                                        database,
                                        ignoreIfExists)));
    }

    /**
     * Drop a database.
     *
     * @param catalogName Name of the catalog for database.
     * @param databaseName Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @param cascade Flag to specify behavior when the database contains table or function: if set
     *     to true, delete all tables and functions in the database and then delete the database, if
     *     set to false, throw an exception.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws DatabaseNotEmptyException if the given database is not empty and isRestrict is true
     * @throws CatalogException in case of any runtime exception
     */
    public void dropDatabase(
            String catalogName, String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        Catalog catalog = getCatalogOrError(catalogName);
        catalog.dropDatabase(databaseName, ignoreIfNotExists, cascade);
        catalogModificationListeners.forEach(
                listener ->
                        listener.onEvent(
                                DropDatabaseEvent.createEvent(
                                        CatalogContext.createContext(catalogName, catalog),
                                        databaseName,
                                        ignoreIfNotExists,
                                        cascade)));
    }

    /**
     * Modify an existing database.
     *
     * @param catalogName Name of the catalog for database
     * @param databaseName Name of the database to be dropped
     * @param newDatabase The new database definition
     * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    public void alterDatabase(
            String catalogName,
            String databaseName,
            CatalogDatabase newDatabase,
            boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        Catalog catalog = getCatalogOrError(catalogName);
        catalog.alterDatabase(databaseName, newDatabase, ignoreIfNotExists);
        catalogModificationListeners.forEach(
                listener ->
                        listener.onEvent(
                                AlterDatabaseEvent.createEvent(
                                        CatalogContext.createContext(catalogName, catalog),
                                        databaseName,
                                        newDatabase,
                                        ignoreIfNotExists)));
    }
}
