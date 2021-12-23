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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionEntry;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionPoolManager;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link JdbcDynamicTableSource}. */
@Internal
public class JdbcRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataAsyncLookupFunction.class);

    private final String query;
    private JdbcConnectionPoolManager jdbcConnectionPoolManager;
    private final DataType[] keyTypes;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cacheMissingKey;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;
    private final int lookupAsyncParallelism;
    private transient ExecutorService asyncExecutors;
    private transient Cache<RowData, List<RowData>> cache;

    public JdbcRowDataAsyncLookupFunction(
            JdbcConnectorOptions options,
            JdbcLookupOptions lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.lookupAsyncParallelism = lookupOptions.getLookupAsyncParallelism();
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.cacheMissingKey = lookupOptions.getCacheMissingKey();
        this.query =
                options.getDialect()
                        .getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
        String dbURL = options.getDbURL();
        this.jdbcDialect = JdbcDialectLoader.load(dbURL);
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter =
                jdbcDialect.getRowConverter(
                        RowType.of(
                                Arrays.stream(keyTypes)
                                        .map(DataType::getLogicalType)
                                        .toArray(LogicalType[]::new)));
        this.jdbcConnectionPoolManager =
                new JdbcConnectionPoolManager(options, lookupAsyncParallelism, keyNames, query);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            establishConnectionAndStatement();
            this.cache =
                    cacheMaxSize == -1 || cacheExpireMs == -1
                            ? null
                            : CacheBuilder.newBuilder()
                                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                                    .maximumSize(cacheMaxSize)
                                    .build();
            // Create async lookup thread pool
            ThreadFactory threadFactory =
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("Flink JDBCAsyncLookupFunction Thread %d")
                            .build();
            asyncExecutors = Executors.newFixedThreadPool(lookupAsyncParallelism, threadFactory);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    private void establishConnectionAndStatement()
            throws SQLException, ClassNotFoundException, InterruptedException {
        for (int i = 0; i < lookupAsyncParallelism; i++) {
            jdbcConnectionPoolManager.createAndAddConnectionEntry();
        }
    }

    public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
        GenericRowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                if (cachedRows.size() == 0) {
                    future.complete(Collections.emptyList());
                } else {
                    for (RowData cachedRow : cachedRows) {
                        future.complete(Collections.singletonList(cachedRow));
                    }
                }
                return;
            }
        }
        // async join
        int currentRetry = 0;
        fetchResult(future, currentRetry, keyRow);
    }

    private void fetchResult(
            CompletableFuture<Collection<RowData>> future,
            int currentRetry,
            GenericRowData keyRow) {
        // get async join resultFuture
        CompletableFuture<Collection<RowData>> rowDataFuture = asyncJdbcExecute(keyRow);

        rowDataFuture.whenCompleteAsync(
                ((rowData, throwable) -> {
                    if (throwable != null) {
                        LOG.error("async jdbc exception,", throwable);
                        if (currentRetry >= maxRetryTimes) {
                            future.completeExceptionally(throwable);
                        } else {
                            try {
                                Thread.sleep(1000 * currentRetry);
                            } catch (InterruptedException e) {
                                throw new RuntimeException("Reestablish JDBC connection failed", e);
                            }
                            fetchResult(future, currentRetry + 1, keyRow);
                        }
                    } else {
                        future.complete(rowData);
                    }
                }));
    }

    private CompletableFuture<Collection<RowData>> asyncJdbcExecute(GenericRowData keyRow) {
        return CompletableFuture.supplyAsync(
                () -> {
                    JdbcConnectionEntry connectionEntry = null;
                    ArrayList<RowData> rows = new ArrayList<>();
                    try {
                        connectionEntry = jdbcConnectionPoolManager.getConnectionEntry();
                        FieldNamedPreparedStatement statement = connectionEntry.getStatement();
                        statement.clearParameters();
                        statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                        ResultSet resultSet = statement.executeQuery();
                        while (resultSet.next()) {
                            RowData row = jdbcRowConverter.toInternal(resultSet);
                            rows.add(row);
                        }
                        rows.trimToSize();
                        if (cache != null) {
                            if (!rows.isEmpty() || cacheMissingKey) {
                                cache.put(keyRow, rows);
                            }
                        }
                        return rows;
                    } catch (SQLException | InterruptedException e) {
                        try {
                            connectionEntry =
                                    jdbcConnectionPoolManager.checkAndCreateConnection(
                                            connectionEntry);
                        } catch (SQLException | ClassNotFoundException | InterruptedException ex) {
                            ex.printStackTrace();
                        }
                        throw new RuntimeException(e.getMessage());
                    } finally {
                        try {
                            jdbcConnectionPoolManager.returnConnectionEntry(connectionEntry);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e.getMessage());
                        }
                    }
                },
                asyncExecutors);
    }

    @Override
    public void close() throws Exception {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        jdbcConnectionPoolManager.closeAll();
        if (asyncExecutors != null && !asyncExecutors.isShutdown()) {
            asyncExecutors.shutdownNow();
        }
    }

    @VisibleForTesting
    public Cache<RowData, List<RowData>> getCache() {
        return cache;
    }
}
