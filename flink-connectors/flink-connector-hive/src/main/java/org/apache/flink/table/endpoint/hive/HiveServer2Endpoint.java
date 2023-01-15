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

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.GatewayInfo;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.api.utils.ThreadUtils;
import org.apache.flink.util.ExceptionUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetCatalogsReq;
import org.apache.hive.service.rpc.thrift.TGetCatalogsResp;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetColumnsResp;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceResp;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsResp;
import org.apache.hive.service.rpc.thrift.TGetInfoReq;
import org.apache.hive.service.rpc.thrift.TGetInfoResp;
import org.apache.hive.service.rpc.thrift.TGetInfoValue;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysResp;
import org.apache.hive.service.rpc.thrift.TGetQueryIdReq;
import org.apache.hive.service.rpc.thrift.TGetQueryIdResp;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasResp;
import org.apache.hive.service.rpc.thrift.TGetTableTypesReq;
import org.apache.hive.service.rpc.thrift.TGetTableTypesResp;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TGetTablesResp;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoReq;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationType;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TSetClientInfoReq;
import org.apache.hive.service.rpc.thrift.TSetClientInfoResp;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;
import static org.apache.flink.table.endpoint.hive.util.HiveJdbcParameterUtils.getUsedDefaultDatabase;
import static org.apache.flink.table.endpoint.hive.util.HiveJdbcParameterUtils.setVariables;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetCatalogsExecutor;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetColumnsExecutor;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetFunctionsExecutor;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetPrimaryKeys;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetSchemasExecutor;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetTableTypesExecutor;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetTablesExecutor;
import static org.apache.flink.table.endpoint.hive.util.OperationExecutorFactory.createGetTypeInfoExecutor;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toFetchOrientation;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toFlinkTableKinds;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toOperationHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toSessionHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationState;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTRowSet;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTSessionHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTStatus;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTTableSchema;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HiveServer2 Endpoint that allows to accept the request from the hive client, e.g. Hive JDBC, Hive
 * Beeline.
 */
public class HiveServer2Endpoint implements TCLIService.Iface, SqlGatewayEndpoint, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveServer2Endpoint.class);
    private static final HiveServer2EndpointVersion SERVER_VERSION = HIVE_CLI_SERVICE_PROTOCOL_V10;
    private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS);
    private static final String UNSUPPORTED_ERROR_MESSAGE =
            "The HiveServer2 Endpoint currently doesn't support to %s.";
    private static final Long CHECK_INTERVAL_MS = 100L;

    // --------------------------------------------------------------------------------------------
    // Server attributes
    // --------------------------------------------------------------------------------------------

    private final SqlGatewayService service;
    private final InetSocketAddress socketAddress;
    private final int minWorkerThreads;
    private final int maxWorkerThreads;
    private final Duration workerKeepAliveTime;
    private final int requestTimeoutMs;
    private final int backOffSlotLengthMs;
    private final long maxMessageSize;
    private final boolean isVerbose;

    private final Thread serverThread = new Thread(this, "HiveServer2 Endpoint");
    private ThreadPoolExecutor executor;
    private TThreadPoolServer server;

    // --------------------------------------------------------------------------------------------
    // Catalog attributes
    // --------------------------------------------------------------------------------------------

    private final String catalogName;
    @Nullable private final String defaultDatabase;
    private final HiveConf hiveConf;
    private final boolean allowEmbedded;

    // --------------------------------------------------------------------------------------------
    // Module attributes
    // --------------------------------------------------------------------------------------------

    private final String moduleName;

    public HiveServer2Endpoint(
            SqlGatewayService service,
            InetSocketAddress socketAddress,
            long maxMessageSize,
            int requestTimeoutMs,
            int backOffSlotLengthMs,
            int minWorkerThreads,
            int maxWorkerThreads,
            Duration workerKeepAliveTime,
            String catalogName,
            HiveConf hiveConf,
            @Nullable String defaultDatabase,
            String moduleName) {
        this(
                service,
                socketAddress,
                maxMessageSize,
                requestTimeoutMs,
                backOffSlotLengthMs,
                minWorkerThreads,
                maxWorkerThreads,
                workerKeepAliveTime,
                catalogName,
                hiveConf,
                defaultDatabase,
                moduleName,
                false,
                true);
    }

    @VisibleForTesting
    public HiveServer2Endpoint(
            SqlGatewayService service,
            InetSocketAddress socketAddress,
            long maxMessageSize,
            int requestTimeoutMs,
            int backOffSlotLengthMs,
            int minWorkerThreads,
            int maxWorkerThreads,
            Duration workerKeepAliveTime,
            String catalogName,
            HiveConf hiveConf,
            @Nullable String defaultDatabase,
            String moduleName,
            boolean allowEmbedded,
            boolean isVerbose) {
        this.service = service;

        this.socketAddress = socketAddress;
        this.maxMessageSize = maxMessageSize;
        this.requestTimeoutMs = requestTimeoutMs;
        this.backOffSlotLengthMs = backOffSlotLengthMs;
        this.minWorkerThreads = minWorkerThreads;
        this.maxWorkerThreads = maxWorkerThreads;
        this.workerKeepAliveTime = checkNotNull(workerKeepAliveTime);
        this.isVerbose = isVerbose;

        this.catalogName = checkNotNull(catalogName);
        this.hiveConf = hiveConf;
        this.defaultDatabase = defaultDatabase;

        this.moduleName = moduleName;

        this.allowEmbedded = allowEmbedded;
    }

    @Override
    public void start() throws Exception {
        buildTThreadPoolServer();
        serverThread.start();
    }

    @Override
    public void stop() throws Exception {
        if (server != null) {
            server.stop();
        }

        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public TOpenSessionResp OpenSession(TOpenSessionReq tOpenSessionReq) throws TException {
        LOG.debug("Client protocol version: {}.", tOpenSessionReq.getClient_protocol());
        TOpenSessionResp resp = new TOpenSessionResp();
        try {
            // negotiate connection protocol
            TProtocolVersion clientProtocol = tOpenSessionReq.getClient_protocol();
            // the session version is not larger than the server version because of the
            // min(server_version, ...)
            HiveServer2EndpointVersion sessionVersion =
                    HiveServer2EndpointVersion.valueOf(
                            TProtocolVersion.findByValue(
                                    Math.min(
                                            clientProtocol.getValue(),
                                            SERVER_VERSION.getVersion().getValue())));

            // prepare session environment
            Map<String, String> originSessionConf =
                    tOpenSessionReq.getConfiguration() == null
                            ? Collections.emptyMap()
                            : tOpenSessionReq.getConfiguration();

            HiveConf conf = new HiveConf(hiveConf);
            Catalog hiveCatalog =
                    new HiveCatalog(
                            catalogName,
                            getUsedDefaultDatabase(originSessionConf).orElse(defaultDatabase),
                            conf,
                            HiveShimLoader.getHiveVersion(),
                            allowEmbedded);
            // Trigger the creation of the HiveMetaStoreClient to use the same HiveConf. If the
            // initial HiveConf is different, it will trigger the PersistenceManagerFactory to close
            // all the alive PersistenceManager in the ObjectStore, which may get error like
            // "Persistence Manager has been closed" in the later connection.
            hiveCatalog.open();
            // create hive module lazily
            SessionEnvironment.ModuleCreator hiveModuleCreator =
                    (readableConfig, classLoader) ->
                            FactoryUtil.createModule(
                                    moduleName,
                                    Collections.emptyMap(),
                                    readableConfig,
                                    classLoader);
            // set variables to HiveConf and Session's conf
            Map<String, String> sessionConfig = new HashMap<>();
            sessionConfig.put(TABLE_SQL_DIALECT.key(), SqlDialect.HIVE.name());
            sessionConfig.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());
            sessionConfig.put(TABLE_DML_SYNC.key(), "true");
            setVariables(conf, sessionConfig, originSessionConf);
            SessionHandle sessionHandle =
                    service.openSession(
                            SessionEnvironment.newBuilder()
                                    .setSessionEndpointVersion(sessionVersion)
                                    .registerCatalogCreator(
                                            catalogName,
                                            (readableConfig, classLoader) -> hiveCatalog)
                                    .registerModuleCreatorAtHead(moduleName, hiveModuleCreator)
                                    .setDefaultCatalog(catalogName)
                                    .addSessionConfig(sessionConfig)
                                    .build());
            // response
            resp.setStatus(OK_STATUS);
            resp.setServerProtocolVersion(sessionVersion.getVersion());
            resp.setSessionHandle(toTSessionHandle(sessionHandle));
            resp.setConfiguration(service.getSessionConfig(sessionHandle));
        } catch (Throwable t) {
            LOG.error("Failed to OpenSession.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TCloseSessionResp CloseSession(TCloseSessionReq tCloseSessionReq) throws TException {
        TCloseSessionResp resp = new TCloseSessionResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tCloseSessionReq.getSessionHandle());
            service.closeSession(sessionHandle);
            resp.setStatus(OK_STATUS);
        } catch (Throwable t) {
            LOG.error("Failed to CloseSession.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
        TGetInfoResp resp = new TGetInfoResp();
        try {
            GatewayInfo info = service.getGatewayInfo();
            TGetInfoValue tInfoValue;
            switch (tGetInfoReq.getInfoType()) {
                case CLI_SERVER_NAME:
                case CLI_DBMS_NAME:
                    tInfoValue = TGetInfoValue.stringValue(info.getProductName());
                    break;
                case CLI_DBMS_VER:
                    tInfoValue = TGetInfoValue.stringValue(info.getVersion().toString());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unrecognized TGetInfoType value: %s.",
                                    tGetInfoReq.getInfoType()));
            }
            resp.setStatus(OK_STATUS);
            resp.setInfoValue(tInfoValue);
        } catch (Throwable t) {
            LOG.error("Failed to GetInfo.", t);
            // InfoValue must be set because the hive service requires it.
            resp.setInfoValue(TGetInfoValue.lenValue(0));
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq)
            throws TException {
        TExecuteStatementResp resp = new TExecuteStatementResp();
        SessionHandle sessionHandle = toSessionHandle(tExecuteStatementReq.getSessionHandle());
        OperationHandle operationHandle = null;
        try {
            String statement =
                    tExecuteStatementReq.isSetStatement()
                            ? tExecuteStatementReq.getStatement()
                            : "";
            Map<String, String> executionConfig =
                    tExecuteStatementReq.isSetConfOverlay()
                            ? tExecuteStatementReq.getConfOverlay()
                            : Collections.emptyMap();
            long timeout = tExecuteStatementReq.getQueryTimeout();

            operationHandle =
                    service.executeStatement(
                            sessionHandle,
                            statement,
                            timeout,
                            Configuration.fromMap(executionConfig));

            if (!tExecuteStatementReq.isRunAsync()) {
                waitUntilOperationIsTerminated(sessionHandle, operationHandle);
            }

            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(
                            sessionHandle, operationHandle, TOperationType.EXECUTE_STATEMENT));
        } catch (Throwable t) {
            LOG.error("Failed to ExecuteStatement.", t);
            resp.setStatus(toTStatus(t));
            if (operationHandle != null) {
                closeOperationSilently(sessionHandle, operationHandle);
            }
        }
        return resp;
    }

    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq tGetTypeInfoReq) throws TException {
        TGetTypeInfoResp resp = new TGetTypeInfoResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetTypeInfoReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(sessionHandle, createGetTypeInfoExecutor());
            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(
                            sessionHandle, operationHandle, TOperationType.GET_TYPE_INFO));
        } catch (Throwable t) {
            LOG.error("Failed to GetTypeInfo.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
        TGetCatalogsResp resp = new TGetCatalogsResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetCatalogsReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(
                            sessionHandle, createGetCatalogsExecutor(service, sessionHandle));
            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(
                            sessionHandle, operationHandle, TOperationType.GET_CATALOGS));
        } catch (Throwable t) {
            LOG.error("Failed to GetCatalogs.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
        TGetSchemasResp resp = new TGetSchemasResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetSchemasReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(
                            sessionHandle,
                            createGetSchemasExecutor(
                                    service,
                                    sessionHandle,
                                    tGetSchemasReq.getCatalogName(),
                                    tGetSchemasReq.getSchemaName()));

            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(sessionHandle, operationHandle, TOperationType.GET_SCHEMAS));
        } catch (Throwable t) {
            LOG.error("Failed to GetSchemas.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
        TGetTablesResp resp = new TGetTablesResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetTablesReq.getSessionHandle());
            Set<TableKind> tableKinds = toFlinkTableKinds(tGetTablesReq.getTableTypes());

            OperationHandle operationHandle =
                    service.submitOperation(
                            sessionHandle,
                            createGetTablesExecutor(
                                    service,
                                    sessionHandle,
                                    tGetTablesReq.getCatalogName(),
                                    tGetTablesReq.getSchemaName(),
                                    tGetTablesReq.getTableName(),
                                    tableKinds));

            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(sessionHandle, operationHandle, TOperationType.GET_TABLES));
        } catch (Throwable t) {
            LOG.error("Failed to GetTables.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
        TGetTableTypesResp resp = new TGetTableTypesResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetTableTypesReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(sessionHandle, createGetTableTypesExecutor());

            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(sessionHandle, operationHandle, TOperationType.GET_TABLES));
        } catch (Throwable t) {
            LOG.error("Failed to GetTableTypes.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
        TGetColumnsResp resp = new TGetColumnsResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetColumnsReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(
                            sessionHandle,
                            createGetColumnsExecutor(
                                    service,
                                    sessionHandle,
                                    tGetColumnsReq.getCatalogName(),
                                    tGetColumnsReq.getSchemaName(),
                                    tGetColumnsReq.getTableName(),
                                    tGetColumnsReq.getColumnName()));

            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(sessionHandle, operationHandle, TOperationType.GET_COLUMNS));
        } catch (Throwable t) {
            LOG.error("Failed to GetColumns.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
        TGetFunctionsResp resp = new TGetFunctionsResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetFunctionsReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(
                            sessionHandle,
                            createGetFunctionsExecutor(
                                    service,
                                    sessionHandle,
                                    tGetFunctionsReq.getCatalogName(),
                                    tGetFunctionsReq.getSchemaName(),
                                    tGetFunctionsReq.getFunctionName()));
            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(
                            sessionHandle, operationHandle, TOperationType.GET_FUNCTIONS));
        } catch (Throwable t) {
            LOG.error("Failed to GetFunctions.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq)
            throws TException {
        TGetPrimaryKeysResp resp = new TGetPrimaryKeysResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tGetPrimaryKeysReq.getSessionHandle());
            OperationHandle operationHandle =
                    service.submitOperation(
                            sessionHandle,
                            createGetPrimaryKeys(
                                    service,
                                    sessionHandle,
                                    tGetPrimaryKeysReq.getCatalogName(),
                                    tGetPrimaryKeysReq.getSchemaName(),
                                    tGetPrimaryKeysReq.getTableName()));

            resp.setStatus(OK_STATUS);
            resp.setOperationHandle(
                    toTOperationHandle(
                            // hive's implementation use "GET_FUNCTIONS" here
                            sessionHandle, operationHandle, TOperationType.GET_FUNCTIONS));
        } catch (Throwable t) {
            LOG.error("Failed to GetPrimaryKeys.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq)
            throws TException {
        return new TGetCrossReferenceResp(buildErrorStatus("GetCrossReference"));
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq)
            throws TException {
        TGetOperationStatusResp resp = new TGetOperationStatusResp();
        try {
            TOperationHandle operationHandle = tGetOperationStatusReq.getOperationHandle();
            OperationInfo operationInfo =
                    service.getOperationInfo(
                            toSessionHandle(operationHandle), toOperationHandle(operationHandle));
            resp.setStatus(OK_STATUS);
            // TODO: support completed time / start time
            resp.setOperationState(toTOperationState(operationInfo.getStatus()));
            // Currently, all operations have results.
            resp.setHasResultSet(true);
            if (operationInfo.getStatus().equals(OperationStatus.ERROR)
                    && operationInfo.getException().isPresent()) {
                resp.setErrorMessage(stringifyException(operationInfo.getException().get()));
            }
        } catch (Throwable t) {
            LOG.error("Failed to GetOperationStatus.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq)
            throws TException {
        TCancelOperationResp resp = new TCancelOperationResp();
        try {
            TOperationHandle operationHandle = tCancelOperationReq.getOperationHandle();
            service.cancelOperation(
                    toSessionHandle(operationHandle), toOperationHandle(operationHandle));
            resp.setStatus(OK_STATUS);
        } catch (Throwable t) {
            LOG.error("Failed to CancelOperation.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq)
            throws TException {
        TCloseOperationResp resp = new TCloseOperationResp();
        try {
            TOperationHandle operationHandle = tCloseOperationReq.getOperationHandle();
            service.closeOperation(
                    toSessionHandle(operationHandle), toOperationHandle(operationHandle));
            resp.setStatus(OK_STATUS);
        } catch (Throwable t) {
            LOG.error("Failed to CloseOperation.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(
            TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
        TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
        try {
            SessionHandle sessionHandle =
                    toSessionHandle(tGetResultSetMetadataReq.getOperationHandle());
            OperationHandle operationHandle =
                    toOperationHandle(tGetResultSetMetadataReq.getOperationHandle());
            ResolvedSchema schema =
                    service.getOperationResultSchema(sessionHandle, operationHandle);

            resp.setStatus(OK_STATUS);
            resp.setSchema(toTTableSchema(schema));
        } catch (Throwable t) {
            LOG.warn("Failed to GetResultSetMetadata.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
        if (tFetchResultsReq.getFetchType() != 0) {
            // Don't log the annoying messages because Hive beeline will fetch the logs until
            // operation is terminated.
            return new TFetchResultsResp(
                    toTStatus(
                            new UnsupportedOperationException(
                                    "The HiveServer2 endpoint currently doesn't support to fetch logs.")));
        }
        TFetchResultsResp resp = new TFetchResultsResp();
        try {
            SessionHandle sessionHandle = toSessionHandle(tFetchResultsReq.getOperationHandle());
            OperationHandle operationHandle =
                    toOperationHandle(tFetchResultsReq.getOperationHandle());
            if (tFetchResultsReq.getMaxRows() > Integer.MAX_VALUE) {
                throw new SqlGatewayException(
                        String.format(
                                "The SqlGateway doesn't support to fetch more that %s rows.",
                                Integer.MAX_VALUE));
            }

            if (tFetchResultsReq.getMaxRows() < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "SqlGateway doesn't support to fetch %s rows. Please specify a positive value for the max rows.",
                                tFetchResultsReq.getMaxRows()));
            }

            int maxRows = (int) tFetchResultsReq.getMaxRows();

            ResultSet resultSet =
                    service.fetchResults(
                            sessionHandle,
                            operationHandle,
                            toFetchOrientation(tFetchResultsReq.getOrientation()),
                            maxRows);
            resp.setStatus(OK_STATUS);
            resp.setHasMoreRows(resultSet.getResultType() != EOS);
            EndpointVersion sessionEndpointVersion =
                    service.getSessionEndpointVersion(sessionHandle);

            if (!(sessionEndpointVersion instanceof HiveServer2EndpointVersion)) {
                throw new SqlGatewayException(
                        String.format(
                                "The specified endpoint version %s is not %s.",
                                sessionEndpointVersion.getClass().getCanonicalName(),
                                HiveServer2EndpointVersion.class.getCanonicalName()));
            }
            resp.setResults(
                    toTRowSet(
                            ((HiveServer2EndpointVersion) sessionEndpointVersion).getVersion(),
                            resultSet.getResultSchema(),
                            resultSet.getData()));

        } catch (Throwable t) {
            LOG.error("Failed to FetchResults.", t);
            resp.setStatus(toTStatus(t));
        }
        return resp;
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq)
            throws TException {
        return new TGetDelegationTokenResp(buildErrorStatus("GetDelegationToken"));
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(
            TCancelDelegationTokenReq tCancelDelegationTokenReq) throws TException {
        return new TCancelDelegationTokenResp(buildErrorStatus("CancelDelegationToken"));
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(
            TRenewDelegationTokenReq tRenewDelegationTokenReq) throws TException {
        return new TRenewDelegationTokenResp(buildErrorStatus("RenewDelegationToken"));
    }

    // CHECKSTYLE.OFF: MethodName
    /** To be compatible with Hive3, add a default implementation. */
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
        throw new TException(
                new UnsupportedOperationException(
                        String.format(UNSUPPORTED_ERROR_MESSAGE, "GetQueryId")));
    }

    /** To be compatible with Hive3, add a default implementation. */
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
        return new TSetClientInfoResp(buildErrorStatus("SetClientInfo"));
    }
    // CHECKSTYLE.ON: MethodName

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveServer2Endpoint)) {
            return false;
        }
        HiveServer2Endpoint that = (HiveServer2Endpoint) o;

        return Objects.equals(socketAddress, that.socketAddress)
                && minWorkerThreads == that.minWorkerThreads
                && maxWorkerThreads == that.maxWorkerThreads
                && requestTimeoutMs == that.requestTimeoutMs
                && backOffSlotLengthMs == that.backOffSlotLengthMs
                && maxMessageSize == that.maxMessageSize
                && Objects.equals(workerKeepAliveTime, that.workerKeepAliveTime)
                && Objects.equals(catalogName, that.catalogName)
                && Objects.equals(defaultDatabase, that.defaultDatabase)
                && Objects.equals(allowEmbedded, that.allowEmbedded)
                && Objects.equals(isVerbose, that.isVerbose)
                && Objects.equals(moduleName, that.moduleName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                socketAddress,
                minWorkerThreads,
                maxWorkerThreads,
                workerKeepAliveTime,
                requestTimeoutMs,
                backOffSlotLengthMs,
                maxMessageSize,
                catalogName,
                defaultDatabase,
                allowEmbedded,
                isVerbose,
                moduleName);
    }

    @Override
    public void run() {
        try {
            LOG.info("HiveServer2 Endpoint begins to listen on {}.", socketAddress.toString());
            server.serve();
        } catch (Throwable t) {
            LOG.error("Exception caught by " + getClass().getSimpleName() + ". Exiting.", t);
            System.exit(-1);
        }
    }

    private void buildTThreadPoolServer() {
        executor =
                ThreadUtils.newThreadPool(
                        minWorkerThreads,
                        maxWorkerThreads,
                        workerKeepAliveTime.toMillis(),
                        "hiveserver2-endpoint-thread-pool");
        try {
            server =
                    new TThreadPoolServer(
                            new TThreadPoolServer.Args(new TServerSocket(socketAddress))
                                    .processorFactory(
                                            new TProcessorFactory(
                                                    new TCLIService.Processor<>(this)))
                                    .transportFactory(new TTransportFactory())
                                    // Currently, only support binary mode.
                                    .protocolFactory(new TBinaryProtocol.Factory())
                                    .inputProtocolFactory(
                                            new TBinaryProtocol.Factory(
                                                    true, true, maxMessageSize, maxMessageSize))
                                    .requestTimeout(requestTimeoutMs)
                                    .requestTimeoutUnit(TimeUnit.MILLISECONDS)
                                    .beBackoffSlotLength(backOffSlotLengthMs)
                                    .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
                                    .executorService(executor));
        } catch (Exception e) {
            throw new SqlGatewayException("Failed to build the server.", e);
        }
    }

    /**
     * Similar solution comparing to the {@code
     * org.apache.hive.jdbc.HiveStatement#waitForOperationToComplete}.
     *
     * <p>The better solution is to introduce an interface similar to {@link TableResult#await()}.
     */
    private void waitUntilOperationIsTerminated(
            SessionHandle sessionHandle, OperationHandle operationHandle) throws Exception {
        OperationInfo info;
        do {
            info = service.getOperationInfo(sessionHandle, operationHandle);
            switch (info.getStatus()) {
                case INITIALIZED:
                case PENDING:
                case RUNNING:
                    Thread.sleep(CHECK_INTERVAL_MS);
                    break;
                case CANCELED:
                case TIMEOUT:
                    throw new SqlGatewayException(
                            String.format(
                                    "The operation %s's status is %s for the session %s.",
                                    operationHandle, info.getStatus(), sessionHandle));
                case ERROR:
                    throw new SqlGatewayException(
                            String.format(
                                    "The operation %s's status is %s for the session %s.",
                                    operationHandle, info.getStatus(), sessionHandle),
                            info.getException()
                                    .orElseThrow(
                                            () ->
                                                    new SqlGatewayException(
                                                            "Impossible! ERROR status should contains the error.")));
                case FINISHED:
                    return;
                default:
                    throw new SqlGatewayException(
                            String.format("Unknown status: %s.", info.getStatus()));
            }
        } while (true);
    }

    private void closeOperationSilently(
            SessionHandle sessionHandle, OperationHandle operationHandle) {
        try {
            service.closeOperation(sessionHandle, operationHandle);
        } catch (Throwable t) {
            // ignore
            LOG.error(
                    String.format(
                            "Close the operation %s for the session %s silently.",
                            operationHandle, sessionHandle),
                    t);
        }
    }

    private String stringifyException(Throwable t) {
        if (isVerbose) {
            return ExceptionUtils.stringifyException(t);
        } else {
            Throwable root = t;
            while (root.getCause() != null
                    && root.getCause().getMessage() != null
                    && !root.getCause().getMessage().isEmpty()) {
                root = root.getCause();
            }
            return root.getClass().getName() + ": " + root.getMessage();
        }
    }

    private TStatus buildErrorStatus(String methodName) {
        return toTStatus(
                new UnsupportedOperationException(
                        String.format(UNSUPPORTED_ERROR_MESSAGE, methodName)));
    }
}
