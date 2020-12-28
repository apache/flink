/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.cli.utils.SqlParserHelper;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.TriFunctionWithException;

import java.util.List;
import java.util.Map;

/** A customizable {@link Executor} for testing purposes. */
class TestingExecutor implements Executor {

    private int numCancelCalls = 0;

    private int numRetrieveResultChancesCalls = 0;
    private final List<
                    SupplierWithException<
                            TypedResult<List<Tuple2<Boolean, Row>>>, SqlExecutionException>>
            resultChanges;

    private int numSnapshotResultCalls = 0;
    private final List<SupplierWithException<TypedResult<Integer>, SqlExecutionException>>
            snapshotResults;

    private int numRetrieveResultPageCalls = 0;
    private final List<SupplierWithException<List<Row>, SqlExecutionException>> resultPages;

    private int numExecuteSqlCalls = 0;
    private final BiFunctionWithException<String, String, TableResult, SqlExecutionException>
            executeSqlConsumer;

    private int numSetSessionPropertyCalls = 0;
    private final TriFunctionWithException<String, String, String, Void, SqlExecutionException>
            setSessionPropertyFunction;

    private int numResetSessionPropertiesCalls = 0;
    private final FunctionWithException<String, Void, SqlExecutionException>
            resetSessionPropertiesFunction;

    private final SqlParserHelper helper;

    TestingExecutor(
            List<
                            SupplierWithException<
                                    TypedResult<List<Tuple2<Boolean, Row>>>, SqlExecutionException>>
                    resultChanges,
            List<SupplierWithException<TypedResult<Integer>, SqlExecutionException>>
                    snapshotResults,
            List<SupplierWithException<List<Row>, SqlExecutionException>> resultPages,
            BiFunctionWithException<String, String, TableResult, SqlExecutionException>
                    executeSqlConsumer,
            TriFunctionWithException<String, String, String, Void, SqlExecutionException>
                    setSessionPropertyFunction,
            FunctionWithException<String, Void, SqlExecutionException>
                    resetSessionPropertiesFunction) {
        this.resultChanges = resultChanges;
        this.snapshotResults = snapshotResults;
        this.resultPages = resultPages;
        this.executeSqlConsumer = executeSqlConsumer;
        this.setSessionPropertyFunction = setSessionPropertyFunction;
        this.resetSessionPropertiesFunction = resetSessionPropertiesFunction;
        helper = new SqlParserHelper();
        helper.registerTables();
    }

    @Override
    public void cancelQuery(String sessionId, String resultId) throws SqlExecutionException {
        numCancelCalls++;
    }

    @Override
    public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(
            String sessionId, String resultId) throws SqlExecutionException {
        return resultChanges
                .get(Math.min(numRetrieveResultChancesCalls++, resultChanges.size() - 1))
                .get();
    }

    @Override
    public List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
        return resultPages
                .get(Math.min(numRetrieveResultPageCalls++, resultPages.size() - 1))
                .get();
    }

    @Override
    public TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize)
            throws SqlExecutionException {
        return snapshotResults
                .get(Math.min(numSnapshotResultCalls++, snapshotResults.size() - 1))
                .get();
    }

    @Override
    public void start() throws SqlExecutionException {}

    @Override
    public String openSession(SessionContext session) throws SqlExecutionException {
        return session.getSessionId();
    }

    @Override
    public void closeSession(String sessionId) throws SqlExecutionException {}

    @Override
    public Map<String, String> getSessionProperties(String sessionId) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void resetSessionProperties(String sessionId) throws SqlExecutionException {
        numResetSessionPropertiesCalls++;
        resetSessionPropertiesFunction.apply(sessionId);
    }

    @Override
    public void setSessionProperty(String sessionId, String key, String value)
            throws SqlExecutionException {
        numSetSessionPropertyCalls++;
        setSessionPropertyFunction.apply(sessionId, key, value);
    }

    @Override
    public TableResult executeSql(String sessionId, String statement) throws SqlExecutionException {
        numExecuteSqlCalls++;
        return executeSqlConsumer.apply(sessionId, statement);
    }

    @Override
    public List<String> listModules(String sessionId) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public Parser getSqlParser(String sessionId) {
        return helper.getSqlParser();
    }

    @Override
    public List<String> completeStatement(String sessionId, String statement, int position) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public ResultDescriptor executeQuery(String sessionId, String query)
            throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public ProgramTargetDescriptor executeUpdate(String sessionId, String statement)
            throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public int getNumCancelCalls() {
        return numCancelCalls;
    }

    public int getNumRetrieveResultChancesCalls() {
        return numRetrieveResultChancesCalls;
    }

    public int getNumSnapshotResultCalls() {
        return numSnapshotResultCalls;
    }

    public int getNumRetrieveResultPageCalls() {
        return numRetrieveResultPageCalls;
    }

    public int getNumExecuteSqlCalls() {
        return numExecuteSqlCalls;
    }

    public int getNumSetSessionPropertyCalls() {
        return numSetSessionPropertyCalls;
    }

    public int getNumResetSessionPropertiesCalls() {
        return numResetSessionPropertiesCalls;
    }
}
