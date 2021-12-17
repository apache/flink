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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.xa.XaFacade.EmptyXaTransactionException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunctionState.of;

/**
 * JDBC sink function that uses XA transactions to provide exactly once guarantees. That is, if a
 * checkpoint succeeds then all records emitted during it are committed in the database, and rolled
 * back otherwise.
 *
 * <p>Each parallel subtask has it's own transactions, independent from other subtasks. Therefore,
 * consistency is only guaranteed within partitions.
 *
 * <p>XA uses a two-phase commit protocol, which solves the consistency problem, but leaves the
 * following issues:
 *
 * <ol>
 *   <li>transactions may be abandoned, holding resources (e.g. locks, versions of rows)
 *   <li>abandoned transactions collide with the new transactions if their IDs repeat after recovery
 *   <li>commit requests may be repeated after job recovery, resulting in error responses and job
 *       failure
 * </ol>
 *
 * <p>The following table summarizes effects of failures during transaction state transitions and
 * ways to mitigate them:
 *
 * <table border="1" style="width:100%;">
 * <col span="1" style="width:15%;">
 * <col span="1" style="width:15%;">
 * <col span="1" style="width:30%;">
 * <col span="1" style="width:40%;">
 * <thead>
 * <tr>
 * <th>Transition</th>
 * <th>Methods</th>
 * <th>What happens if transition lost</th>
 * <th>Ways to mitigate</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 * <td>none &gt; started, started &gt; ended</td>
 * <td>open(), snapshotState()</td>
 * <td>Database eventually discards these transactions</td>
 * <td><ol>
 * <li>Use globally unique XIDs</li>
 * <li>derive XID from: checkpoint id, subtask id, "job id", "run id" (see {@link SemanticXidGenerator}).</li>
 * </ol></td>
 * </tr>
 * <tr>
 * <td>ended &gt; prepared</td>
 * <td>snapshotState()</td>
 * <td>Database keeps these transactions prepared forever ("in-doubt" state)</td>
 * <td>
 * <ol>
 * <li>store ended transactions in state; rollback on job recovery (still doesn't cover all scenarios)</li>
 * <li>call xa_recover() and xa_rollback() on job recovery; disabled by default in order not to affect transactions of other subtasks and apps</li>
 * <li>setting transaction timeouts (not supported by most databases)</li>
 * <li>manual recovery and rollback</li>
 * </ol>
 * </td>
 * </tr>
 * <tr>
 * <td>prepared &gt; committed</td>
 * <td>open(), notifyCheckpointComplete()</td>
 * <td>
 * Upon job recovery state contains committed transactions; or JM may notifyCheckpointComplete again after recovery.
 * <p>Committing results in {@link javax.transaction.xa.XAException#XAER_NOTA XAER_NOTA} error.</p>
 * </td>
 * <td>
 * Distinguish between transactions created during this run and restored from state and ignore {@link javax.transaction.xa.XAException#XAER_NOTA XAER_NOTA} for the latter.
 * </td>
 * </tr>
 * </tbody>
 * </table>
 *
 * @since 1.13
 */
@Internal
public class JdbcXaSinkFunction<T> extends AbstractRichFunction
        implements CheckpointedFunction,
                CheckpointListener,
                SinkFunction<T>,
                AutoCloseable,
                InputTypeConfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcXaSinkFunction.class);

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final XidGenerator xidGenerator;
    private final JdbcOutputFormat<T, T, JdbcBatchStatementExecutor<T>> outputFormat;
    private final XaSinkStateHandler stateHandler;
    private final JdbcExactlyOnceOptions options;

    // checkpoints and the corresponding transactions waiting for completion notification from JM
    private transient List<CheckpointAndXid> preparedXids = new ArrayList<>();
    // hanging XIDs - used for cleanup
    // it's a list to support retries and scaling down
    // possible transaction states: active, idle, prepared
    // last element is the current xid
    private transient Deque<Xid> hangingXids = new LinkedList<>();
    private transient Xid currentXid;

    /**
     * Creates a {@link JdbcXaSinkFunction}.
     *
     * <p>All parameters must be {@link java.io.Serializable serializable}.
     *
     * @param xaFacade {@link XaFacade} to manage XA transactions
     */
    public JdbcXaSinkFunction(
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            XaFacade xaFacade,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions options) {
        this(
                new JdbcOutputFormat<>(
                        xaFacade,
                        executionOptions,
                        context ->
                                JdbcBatchStatementExecutor.simple(
                                        sql, statementBuilder, Function.identity()),
                        JdbcOutputFormat.RecordExtractor.identity()),
                xaFacade,
                XidGenerator.semanticXidGenerator(),
                new XaSinkStateHandlerImpl(),
                options,
                new XaGroupOpsImpl(xaFacade));
    }

    /**
     * Creates a {@link JdbcXaSinkFunction}.
     *
     * <p>All parameters must be {@link java.io.Serializable serializable}.
     *
     * @param outputFormat {@link JdbcOutputFormat} to write records with
     * @param xaFacade {@link XaFacade} to manage XA transactions
     * @param xidGenerator {@link XidGenerator} to generate new transaction ids
     */
    public JdbcXaSinkFunction(
            JdbcOutputFormat<T, T, JdbcBatchStatementExecutor<T>> outputFormat,
            XaFacade xaFacade,
            XidGenerator xidGenerator,
            XaSinkStateHandler stateHandler,
            JdbcExactlyOnceOptions options,
            XaGroupOps xaGroupOps) {
        this.xaFacade = Preconditions.checkNotNull(xaFacade);
        this.xidGenerator = Preconditions.checkNotNull(xidGenerator);
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
        this.stateHandler = Preconditions.checkNotNull(stateHandler);
        this.options = Preconditions.checkNotNull(options);
        this.xaGroupOps = xaGroupOps;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        JdbcXaSinkFunctionState state = stateHandler.load(context);
        hangingXids = new LinkedList<>(state.getHanging());
        preparedXids = new ArrayList<>(state.getPrepared());
        LOG.info(
                "initialized state: prepared xids: {}, hanging xids: {}",
                preparedXids.size(),
                hangingXids.size());
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        xidGenerator.open();
        xaFacade.open();
        hangingXids = new LinkedList<>(xaGroupOps.failOrRollback(hangingXids).getForRetry());
        commitUpToCheckpoint(Optional.empty());
        if (options.isDiscoverAndRollbackOnRecovery()) {
            // Pending transactions which are not included into the checkpoint might hold locks and
            // should be rolled back. However, rolling back ALL transactions can cause data loss. So
            // each subtask first commits transactions from its state and then rolls back discovered
            // transactions if they belong to it.
            xaGroupOps.recoverAndRollback(getRuntimeContext(), xidGenerator);
        }
        beginTx(0L);
        outputFormat.setRuntimeContext(getRuntimeContext());
        // open format only after starting the transaction so it gets a ready to  use connection
        outputFormat.open(
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("snapshot state, checkpointId={}", context.getCheckpointId());
        prepareCurrentTx(context.getCheckpointId());
        beginTx(context.getCheckpointId() + 1);
        stateHandler.store(of(preparedXids, hangingXids));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        commitUpToCheckpoint(Optional.of(checkpointId));
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        Preconditions.checkState(currentXid != null, "current xid must not be null");
        if (LOG.isTraceEnabled()) {
            LOG.trace("invoke, xid: {}, value: {}", currentXid, value);
        }
        outputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
            }
        }
        xaFacade.close();
        xidGenerator.close();
        // don't format.close(); as we don't want neither to flush nor to close connection here
        currentXid = null;
        hangingXids = null;
        preparedXids = null;
    }

    private void prepareCurrentTx(long checkpointId) throws IOException {
        Preconditions.checkState(currentXid != null, "no current xid");
        Preconditions.checkState(
                !hangingXids.isEmpty() && hangingXids.peekLast().equals(currentXid),
                "inconsistent internal state");
        hangingXids.pollLast();
        outputFormat.flush();
        try {
            xaFacade.endAndPrepare(currentXid);
            preparedXids.add(CheckpointAndXid.createNew(checkpointId, currentXid));
        } catch (EmptyXaTransactionException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}, checkpoint {}",
                    currentXid,
                    checkpointId);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentXid = null;
    }

    /** @param checkpointId to associate with the new transaction. */
    private void beginTx(long checkpointId) throws Exception {
        Preconditions.checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(getRuntimeContext(), checkpointId);
        hangingXids.offerLast(currentXid);
        xaFacade.start(currentXid);
        if (checkpointId > 0) {
            // associate outputFormat with a new connection that might have been opened in start()
            outputFormat.updateExecutor(false);
        }
    }

    private void commitUpToCheckpoint(Optional<Long> checkpointInclusive) {
        Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> splittedXids =
                split(preparedXids, checkpointInclusive, true);
        if (splittedXids.f0.isEmpty()) {
            checkpointInclusive.ifPresent(
                    cp -> LOG.warn("nothing to commit up to checkpoint: {}", cp));
        } else {
            preparedXids = splittedXids.f1;
            preparedXids.addAll(
                    xaGroupOps
                            .commit(
                                    splittedXids.f0,
                                    options.isAllowOutOfOrderCommits(),
                                    options.getMaxCommitAttempts())
                            .getForRetry());
        }
    }

    private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> split(
            List<CheckpointAndXid> list,
            Optional<Long> checkpointInclusive,
            boolean checkpointIntoLo) {
        return checkpointInclusive
                .map(cp -> split(preparedXids, cp, checkpointIntoLo))
                .orElse(new Tuple2<>(list, new ArrayList<>()));
    }

    private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> split(
            List<CheckpointAndXid> list, long checkpoint, boolean checkpointIntoLo) {
        List<CheckpointAndXid> lo = new ArrayList<>(list.size() / 2);
        List<CheckpointAndXid> hi = new ArrayList<>(list.size() / 2);
        list.forEach(
                i -> {
                    if (i.checkpointId < checkpoint
                            || (i.checkpointId == checkpoint && checkpointIntoLo)) {
                        lo.add(i);
                    } else {
                        hi.add(i);
                    }
                });
        return new Tuple2<>(lo, hi);
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        outputFormat.setInputType(type, executionConfig);
    }
}
