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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This is a recommended base class for all of the {@link SinkFunction} that intend to implement
 * exactly-once semantic. It does that by implementing two phase commit algorithm on top of the
 * {@link CheckpointedFunction} and {@link CheckpointListener}. User should provide custom {@code
 * TXN} (transaction handle) and implement abstract methods handling this transaction handle.
 *
 * @param <IN> Input type for {@link SinkFunction}.
 * @param <TXN> Transaction to store all of the information required to handle a transaction.
 * @param <CONTEXT> Context that will be shared across all invocations for the given {@link
 *     TwoPhaseCommitSinkFunction} instance. Context is created once
 */
@PublicEvolving
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitSinkFunction.class);

    protected final LinkedHashMap<Long, TransactionHolder<TXN>> pendingCommitTransactions =
            new LinkedHashMap<>();

    protected transient Optional<CONTEXT> userContext;

    protected transient ListState<State<TXN, CONTEXT>> state;

    private final Clock clock;

    private final ListStateDescriptor<State<TXN, CONTEXT>> stateDescriptor;

    private TransactionHolder<TXN> currentTransactionHolder;

    /** Specifies the maximum time a transaction should remain open. */
    private long transactionTimeout = Long.MAX_VALUE;

    /**
     * If true, any exception thrown in {@link #recoverAndCommit(Object)} will be caught instead of
     * propagated.
     */
    private boolean ignoreFailuresAfterTransactionTimeout;

    /**
     * If a transaction's elapsed time reaches this percentage of the transactionTimeout, a warning
     * message will be logged. Value must be in range [0,1]. Negative value disables warnings.
     */
    private double transactionTimeoutWarningRatio = -1;

    /**
     * Use default {@link ListStateDescriptor} for internal state serialization. Helpful utilities
     * for using this constructor are {@link TypeInformation#of(Class)}, {@link
     * org.apache.flink.api.common.typeinfo.TypeHint} and {@link TypeInformation#of(TypeHint)}.
     * Example:
     *
     * <pre>{@code
     * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
     * }</pre>
     *
     * @param transactionSerializer {@link TypeSerializer} for the transaction type of this sink
     * @param contextSerializer {@link TypeSerializer} for the context type of this sink
     */
    public TwoPhaseCommitSinkFunction(
            TypeSerializer<TXN> transactionSerializer, TypeSerializer<CONTEXT> contextSerializer) {
        this(transactionSerializer, contextSerializer, Clock.systemUTC());
    }

    @VisibleForTesting
    TwoPhaseCommitSinkFunction(
            TypeSerializer<TXN> transactionSerializer,
            TypeSerializer<CONTEXT> contextSerializer,
            Clock clock) {
        this.stateDescriptor =
                new ListStateDescriptor<>(
                        "state", new StateSerializer<>(transactionSerializer, contextSerializer));
        this.clock = clock;
    }

    protected Optional<CONTEXT> initializeUserContext() {
        return Optional.empty();
    }

    protected Optional<CONTEXT> getUserContext() {
        return userContext;
    }

    @Nullable
    protected TXN currentTransaction() {
        return currentTransactionHolder == null ? null : currentTransactionHolder.handle;
    }

    @Nonnull
    protected Stream<Map.Entry<Long, TXN>> pendingTransactions() {
        return pendingCommitTransactions.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().handle));
    }

    // ------ methods that should be implemented in child class to support two phase commit
    // algorithm ------

    /** Write value within a transaction. */
    protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;

    /**
     * Method that starts a new transaction.
     *
     * @return newly created transaction.
     */
    protected abstract TXN beginTransaction() throws Exception;

    /**
     * Pre commit previously created transaction. Pre commit must make all of the necessary steps to
     * prepare the transaction for a commit that might happen in the future. After this point the
     * transaction might still be aborted, but underlying implementation must ensure that commit
     * calls on already pre committed transactions will always succeed.
     *
     * <p>Usually implementation involves flushing the data.
     */
    protected abstract void preCommit(TXN transaction) throws Exception;

    /**
     * Commit a pre-committed transaction. If this method fail, Flink application will be restarted
     * and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
     * same transaction.
     */
    protected abstract void commit(TXN transaction);

    /**
     * Invoked on recovered transactions after a failure. User implementation must ensure that this
     * call will eventually succeed. If it fails, Flink application will be restarted and it will be
     * invoked again. If it does not succeed eventually, a data loss will occur. Transactions will
     * be recovered in an order in which they were created.
     */
    protected void recoverAndCommit(TXN transaction) {
        commit(transaction);
    }

    /** Abort a transaction. */
    protected abstract void abort(TXN transaction);

    /** Abort a transaction that was rejected by a coordinator after a failure. */
    protected void recoverAndAbort(TXN transaction) {
        abort(transaction);
    }

    /**
     * Callback for subclasses which is called after restoring (each) user context.
     *
     * @param handledTransactions transactions which were already committed or aborted and do not
     *     need further handling
     */
    protected void finishRecoveringContext(Collection<TXN> handledTransactions) {}

    // ------ entry points for above methods implementing {@CheckPointedFunction} and
    // {@CheckpointListener} ------

    /** This should not be implemented by subclasses. */
    @Override
    public final void invoke(IN value) throws Exception {}

    @Override
    public final void invoke(IN value, Context context) throws Exception {
        invoke(currentTransactionHolder.handle, value, context);
    }

    @Override
    public final void notifyCheckpointComplete(long checkpointId) throws Exception {
        // the following scenarios are possible here
        //
        //  (1) there is exactly one transaction from the latest checkpoint that
        //      was triggered and completed. That should be the common case.
        //      Simply commit that transaction in that case.
        //
        //  (2) there are multiple pending transactions because one previous
        //      checkpoint was skipped. That is a rare case, but can happen
        //      for example when:
        //
        //        - the master cannot persist the metadata of the last
        //          checkpoint (temporary outage in the storage system) but
        //          could persist a successive checkpoint (the one notified here)
        //
        //        - other tasks could not persist their status during
        //          the previous checkpoint, but did not trigger a failure because they
        //          could hold onto their state and could successfully persist it in
        //          a successive checkpoint (the one notified here)
        //
        //      In both cases, the prior checkpoint never reach a committed state, but
        //      this checkpoint is always expected to subsume the prior one and cover all
        //      changes since the last successful one. As a consequence, we need to commit
        //      all pending transactions.
        //
        //  (3) Multiple transactions are pending, but the checkpoint complete notification
        //      relates not to the latest. That is possible, because notification messages
        //      can be delayed (in an extreme case till arrive after a succeeding checkpoint
        //      was triggered) and because there can be concurrent overlapping checkpoints
        //      (a new one is started before the previous fully finished).
        //
        // ==> There should never be a case where we have no pending transaction here
        //

        Iterator<Map.Entry<Long, TransactionHolder<TXN>>> pendingTransactionIterator =
                pendingCommitTransactions.entrySet().iterator();
        Throwable firstError = null;

        while (pendingTransactionIterator.hasNext()) {
            Map.Entry<Long, TransactionHolder<TXN>> entry = pendingTransactionIterator.next();
            Long pendingTransactionCheckpointId = entry.getKey();
            TransactionHolder<TXN> pendingTransaction = entry.getValue();
            if (pendingTransactionCheckpointId > checkpointId) {
                continue;
            }

            LOG.info(
                    "{} - checkpoint {} complete, committing transaction {} from checkpoint {}",
                    name(),
                    checkpointId,
                    pendingTransaction,
                    pendingTransactionCheckpointId);

            logWarningIfTimeoutAlmostReached(pendingTransaction);
            try {
                commit(pendingTransaction.handle);
            } catch (Throwable t) {
                if (firstError == null) {
                    firstError = t;
                }
            }

            LOG.debug("{} - committed checkpoint transaction {}", name(), pendingTransaction);

            pendingTransactionIterator.remove();
        }

        if (firstError != null) {
            throw new FlinkRuntimeException(
                    "Committing one of transactions failed, logging first encountered failure",
                    firstError);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // this is like the pre-commit of a 2-phase-commit transaction
        // we are ready to commit and remember the transaction

        checkState(
                currentTransactionHolder != null,
                "bug: no transaction object when performing state snapshot");

        long checkpointId = context.getCheckpointId();
        LOG.debug(
                "{} - checkpoint {} triggered, flushing transaction '{}'",
                name(),
                context.getCheckpointId(),
                currentTransactionHolder);

        preCommit(currentTransactionHolder.handle);
        pendingCommitTransactions.put(checkpointId, currentTransactionHolder);
        LOG.debug("{} - stored pending transactions {}", name(), pendingCommitTransactions);

        currentTransactionHolder = beginTransactionInternal();
        LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);

        state.clear();
        state.add(
                new State<>(
                        this.currentTransactionHolder,
                        new ArrayList<>(pendingCommitTransactions.values()),
                        userContext));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // when we are restoring state with pendingCommitTransactions, we don't really know whether
        // the
        // transactions were already committed, or whether there was a failure between
        // completing the checkpoint on the master, and notifying the writer here.

        // (the common case is actually that is was already committed, the window
        // between the commit on the master and the notification here is very small)

        // it is possible to not have any transactions at all if there was a failure before
        // the first completed checkpoint, or in case of a scale-out event, where some of the
        // new task do not have and transactions assigned to check)

        // we can have more than one transaction to check in case of a scale-in event, or
        // for the reasons discussed in the 'notifyCheckpointComplete()' method.

        state = context.getOperatorStateStore().getListState(stateDescriptor);

        boolean recoveredUserContext = false;
        if (context.isRestored()) {
            LOG.info("{} - restoring state", name());
            for (State<TXN, CONTEXT> operatorState : state.get()) {
                userContext = operatorState.getContext();
                List<TransactionHolder<TXN>> recoveredTransactions =
                        operatorState.getPendingCommitTransactions();
                List<TXN> handledTransactions = new ArrayList<>(recoveredTransactions.size() + 1);
                for (TransactionHolder<TXN> recoveredTransaction : recoveredTransactions) {
                    // If this fails to succeed eventually, there is actually data loss
                    recoverAndCommitInternal(recoveredTransaction);
                    handledTransactions.add(recoveredTransaction.handle);
                    LOG.info("{} committed recovered transaction {}", name(), recoveredTransaction);
                }

                {
                    TXN transaction = operatorState.getPendingTransaction().handle;
                    recoverAndAbort(transaction);
                    handledTransactions.add(transaction);
                    LOG.info(
                            "{} aborted recovered transaction {}",
                            name(),
                            operatorState.getPendingTransaction());
                }

                if (userContext.isPresent()) {
                    finishRecoveringContext(handledTransactions);
                    recoveredUserContext = true;
                }
            }
        }

        // if in restore we didn't get any userContext or we are initializing from scratch
        if (!recoveredUserContext) {
            LOG.info("{} - no state to restore", name());

            userContext = initializeUserContext();
        }
        this.pendingCommitTransactions.clear();

        currentTransactionHolder = beginTransactionInternal();
        LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);
    }

    /**
     * This method must be the only place to call {@link #beginTransaction()} to ensure that the
     * {@link TransactionHolder} is created at the same time.
     */
    private TransactionHolder<TXN> beginTransactionInternal() throws Exception {
        return new TransactionHolder<>(beginTransaction(), clock.millis());
    }

    /**
     * This method must be the only place to call {@link #recoverAndCommit(Object)} to ensure that
     * the configuration parameters {@link #transactionTimeout} and {@link
     * #ignoreFailuresAfterTransactionTimeout} are respected.
     */
    private void recoverAndCommitInternal(TransactionHolder<TXN> transactionHolder) {
        try {
            logWarningIfTimeoutAlmostReached(transactionHolder);
            recoverAndCommit(transactionHolder.handle);
        } catch (final Exception e) {
            final long elapsedTime = clock.millis() - transactionHolder.transactionStartTime;
            if (ignoreFailuresAfterTransactionTimeout && elapsedTime > transactionTimeout) {
                LOG.error(
                        "Error while committing transaction {}. "
                                + "Transaction has been open for longer than the transaction timeout ({})."
                                + "Commit will not be attempted again. Data loss might have occurred.",
                        transactionHolder.handle,
                        transactionTimeout,
                        e);
            } else {
                throw e;
            }
        }
    }

    private void logWarningIfTimeoutAlmostReached(TransactionHolder<TXN> transactionHolder) {
        final long elapsedTime = transactionHolder.elapsedTime(clock);
        if (transactionTimeoutWarningRatio >= 0
                && elapsedTime > transactionTimeout * transactionTimeoutWarningRatio) {
            LOG.warn(
                    "Transaction {} has been open for {} ms. "
                            + "This is close to or even exceeding the transaction timeout of {} ms.",
                    transactionHolder.handle,
                    elapsedTime,
                    transactionTimeout);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (currentTransactionHolder != null) {
            abort(currentTransactionHolder.handle);
            currentTransactionHolder = null;
        }
    }

    /**
     * Sets the transaction timeout. Setting only the transaction timeout has no effect in itself.
     *
     * @param transactionTimeout The transaction timeout in ms.
     * @see #ignoreFailuresAfterTransactionTimeout()
     * @see #enableTransactionTimeoutWarnings(double)
     */
    protected TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> setTransactionTimeout(
            long transactionTimeout) {
        checkArgument(transactionTimeout >= 0, "transactionTimeout must not be negative");
        this.transactionTimeout = transactionTimeout;
        return this;
    }

    /**
     * If called, the sink will only log but not propagate exceptions thrown in {@link
     * #recoverAndCommit(Object)} if the transaction is older than a specified transaction timeout.
     * The start time of an transaction is determined by {@link System#currentTimeMillis()}. By
     * default, failures are propagated.
     */
    protected TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> ignoreFailuresAfterTransactionTimeout() {
        this.ignoreFailuresAfterTransactionTimeout = true;
        return this;
    }

    /**
     * Enables logging of warnings if a transaction's elapsed time reaches a specified ratio of the
     * <code>transactionTimeout</code>. If <code>warningRatio</code> is 0, a warning will be always
     * logged when committing the transaction.
     *
     * @param warningRatio A value in the range [0,1].
     * @return
     */
    protected TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> enableTransactionTimeoutWarnings(
            double warningRatio) {
        checkArgument(
                warningRatio >= 0 && warningRatio <= 1, "warningRatio must be in range [0,1]");
        this.transactionTimeoutWarningRatio = warningRatio;
        return this;
    }

    private String name() {
        return String.format(
                "%s %s/%s",
                this.getClass().getSimpleName(),
                getRuntimeContext().getIndexOfThisSubtask() + 1,
                getRuntimeContext().getNumberOfParallelSubtasks());
    }

    /** State POJO class coupling pendingTransaction, context and pendingCommitTransactions. */
    @VisibleForTesting
    @Internal
    public static final class State<TXN, CONTEXT> {
        protected TransactionHolder<TXN> pendingTransaction;
        protected List<TransactionHolder<TXN>> pendingCommitTransactions = new ArrayList<>();
        protected Optional<CONTEXT> context;

        public State() {}

        public State(
                TransactionHolder<TXN> pendingTransaction,
                List<TransactionHolder<TXN>> pendingCommitTransactions,
                Optional<CONTEXT> context) {
            this.context = requireNonNull(context, "context is null");
            this.pendingTransaction =
                    requireNonNull(pendingTransaction, "pendingTransaction is null");
            this.pendingCommitTransactions =
                    requireNonNull(pendingCommitTransactions, "pendingCommitTransactions is null");
        }

        public TransactionHolder<TXN> getPendingTransaction() {
            return pendingTransaction;
        }

        public void setPendingTransaction(TransactionHolder<TXN> pendingTransaction) {
            this.pendingTransaction = pendingTransaction;
        }

        public List<TransactionHolder<TXN>> getPendingCommitTransactions() {
            return pendingCommitTransactions;
        }

        public void setPendingCommitTransactions(
                List<TransactionHolder<TXN>> pendingCommitTransactions) {
            this.pendingCommitTransactions = pendingCommitTransactions;
        }

        public Optional<CONTEXT> getContext() {
            return context;
        }

        public void setContext(Optional<CONTEXT> context) {
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            State<?, ?> state = (State<?, ?>) o;

            if (pendingTransaction != null
                    ? !pendingTransaction.equals(state.pendingTransaction)
                    : state.pendingTransaction != null) {
                return false;
            }
            if (pendingCommitTransactions != null
                    ? !pendingCommitTransactions.equals(state.pendingCommitTransactions)
                    : state.pendingCommitTransactions != null) {
                return false;
            }
            return context != null ? context.equals(state.context) : state.context == null;
        }

        @Override
        public int hashCode() {
            int result = pendingTransaction != null ? pendingTransaction.hashCode() : 0;
            result =
                    31 * result
                            + (pendingCommitTransactions != null
                                    ? pendingCommitTransactions.hashCode()
                                    : 0);
            result = 31 * result + (context != null ? context.hashCode() : 0);
            return result;
        }
    }

    /**
     * Adds metadata (currently only the start time of the transaction) to the transaction object.
     */
    @VisibleForTesting
    @Internal
    public static final class TransactionHolder<TXN> {

        private final TXN handle;

        /**
         * The system time when {@link #handle} was created. Used to determine if the current
         * transaction has exceeded its timeout specified by {@link #transactionTimeout}.
         */
        private final long transactionStartTime;

        @VisibleForTesting
        public TransactionHolder(TXN handle, long transactionStartTime) {
            this.handle = handle;
            this.transactionStartTime = transactionStartTime;
        }

        long elapsedTime(Clock clock) {
            return clock.millis() - transactionStartTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TransactionHolder<?> that = (TransactionHolder<?>) o;

            if (transactionStartTime != that.transactionStartTime) {
                return false;
            }
            return handle != null ? handle.equals(that.handle) : that.handle == null;
        }

        @Override
        public int hashCode() {
            int result = handle != null ? handle.hashCode() : 0;
            result = 31 * result + (int) (transactionStartTime ^ (transactionStartTime >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "TransactionHolder{"
                    + "handle="
                    + handle
                    + ", transactionStartTime="
                    + transactionStartTime
                    + '}';
        }
    }

    /** Custom {@link TypeSerializer} for the sink state. */
    @VisibleForTesting
    @Internal
    public static final class StateSerializer<TXN, CONTEXT>
            extends TypeSerializer<State<TXN, CONTEXT>> {

        private static final long serialVersionUID = 1L;

        private final TypeSerializer<TXN> transactionSerializer;
        private final TypeSerializer<CONTEXT> contextSerializer;

        public StateSerializer(
                TypeSerializer<TXN> transactionSerializer,
                TypeSerializer<CONTEXT> contextSerializer) {
            this.transactionSerializer = checkNotNull(transactionSerializer);
            this.contextSerializer = checkNotNull(contextSerializer);
        }

        @Override
        public boolean isImmutableType() {
            return transactionSerializer.isImmutableType() && contextSerializer.isImmutableType();
        }

        @Override
        public TypeSerializer<State<TXN, CONTEXT>> duplicate() {
            return new StateSerializer<>(
                    transactionSerializer.duplicate(), contextSerializer.duplicate());
        }

        @Override
        public State<TXN, CONTEXT> createInstance() {
            return null;
        }

        @Override
        public State<TXN, CONTEXT> copy(State<TXN, CONTEXT> from) {
            final TransactionHolder<TXN> pendingTransaction = from.getPendingTransaction();
            final TransactionHolder<TXN> copiedPendingTransaction =
                    new TransactionHolder<>(
                            transactionSerializer.copy(pendingTransaction.handle),
                            pendingTransaction.transactionStartTime);

            final List<TransactionHolder<TXN>> copiedPendingCommitTransactions = new ArrayList<>();
            for (TransactionHolder<TXN> txn : from.getPendingCommitTransactions()) {
                final TXN txnHandleCopy = transactionSerializer.copy(txn.handle);
                copiedPendingCommitTransactions.add(
                        new TransactionHolder<>(txnHandleCopy, txn.transactionStartTime));
            }

            final Optional<CONTEXT> copiedContext = from.getContext().map(contextSerializer::copy);
            return new State<>(
                    copiedPendingTransaction, copiedPendingCommitTransactions, copiedContext);
        }

        @Override
        public State<TXN, CONTEXT> copy(State<TXN, CONTEXT> from, State<TXN, CONTEXT> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(State<TXN, CONTEXT> record, DataOutputView target)
                throws IOException {
            final TransactionHolder<TXN> pendingTransaction = record.getPendingTransaction();
            transactionSerializer.serialize(pendingTransaction.handle, target);
            target.writeLong(pendingTransaction.transactionStartTime);

            final List<TransactionHolder<TXN>> pendingCommitTransactions =
                    record.getPendingCommitTransactions();
            target.writeInt(pendingCommitTransactions.size());
            for (TransactionHolder<TXN> pendingTxn : pendingCommitTransactions) {
                transactionSerializer.serialize(pendingTxn.handle, target);
                target.writeLong(pendingTxn.transactionStartTime);
            }

            Optional<CONTEXT> context = record.getContext();
            if (context.isPresent()) {
                target.writeBoolean(true);
                contextSerializer.serialize(context.get(), target);
            } else {
                target.writeBoolean(false);
            }
        }

        @Override
        public State<TXN, CONTEXT> deserialize(DataInputView source) throws IOException {
            TXN pendingTxnHandle = transactionSerializer.deserialize(source);
            final long pendingTxnStartTime = source.readLong();
            final TransactionHolder<TXN> pendingTxn =
                    new TransactionHolder<>(pendingTxnHandle, pendingTxnStartTime);

            int numPendingCommitTxns = source.readInt();
            List<TransactionHolder<TXN>> pendingCommitTxns = new ArrayList<>(numPendingCommitTxns);
            for (int i = 0; i < numPendingCommitTxns; i++) {
                final TXN pendingCommitTxnHandle = transactionSerializer.deserialize(source);
                final long pendingCommitTxnStartTime = source.readLong();
                pendingCommitTxns.add(
                        new TransactionHolder<>(pendingCommitTxnHandle, pendingCommitTxnStartTime));
            }

            Optional<CONTEXT> context = Optional.empty();
            boolean hasContext = source.readBoolean();
            if (hasContext) {
                context = Optional.of(contextSerializer.deserialize(source));
            }

            return new State<>(pendingTxn, pendingCommitTxns, context);
        }

        @Override
        public State<TXN, CONTEXT> deserialize(State<TXN, CONTEXT> reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            TXN pendingTxnHandle = transactionSerializer.deserialize(source);
            transactionSerializer.serialize(pendingTxnHandle, target);
            final long pendingTxnStartTime = source.readLong();
            target.writeLong(pendingTxnStartTime);

            int numPendingCommitTxns = source.readInt();
            target.writeInt(numPendingCommitTxns);
            for (int i = 0; i < numPendingCommitTxns; i++) {
                TXN pendingCommitTxnHandle = transactionSerializer.deserialize(source);
                transactionSerializer.serialize(pendingCommitTxnHandle, target);
                final long pendingCommitTxnStartTime = source.readLong();
                target.writeLong(pendingCommitTxnStartTime);
            }

            boolean hasContext = source.readBoolean();
            target.writeBoolean(hasContext);
            if (hasContext) {
                CONTEXT context = contextSerializer.deserialize(source);
                contextSerializer.serialize(context, target);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StateSerializer<?, ?> that = (StateSerializer<?, ?>) o;

            if (!transactionSerializer.equals(that.transactionSerializer)) {
                return false;
            }
            return contextSerializer.equals(that.contextSerializer);
        }

        @Override
        public int hashCode() {
            int result = transactionSerializer.hashCode();
            result = 31 * result + contextSerializer.hashCode();
            return result;
        }

        @Override
        public StateSerializerSnapshot<TXN, CONTEXT> snapshotConfiguration() {
            return new StateSerializerSnapshot<>(this);
        }
    }

    /**
     * {@link TypeSerializerConfigSnapshot} for sink state. This has to be public so that it can be
     * deserialized/instantiated, should not be used anywhere outside {@code
     * TwoPhaseCommitSinkFunction}.
     *
     * @deprecated this snapshot class is no longer in use, and is maintained only for backwards
     *     compatibility purposes. It is fully replaced by {@link StateSerializerSnapshot}.
     */
    @Internal
    @Deprecated
    public static final class StateSerializerConfigSnapshot<TXN, CONTEXT>
            extends CompositeTypeSerializerConfigSnapshot<State<TXN, CONTEXT>> {

        private static final int VERSION = 1;

        /** This empty nullary constructor is required for deserializing the configuration. */
        public StateSerializerConfigSnapshot() {}

        public StateSerializerConfigSnapshot(
                TypeSerializer<TXN> transactionSerializer,
                TypeSerializer<CONTEXT> contextSerializer) {
            super(transactionSerializer, contextSerializer);
        }

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public TypeSerializerSchemaCompatibility<State<TXN, CONTEXT>> resolveSchemaCompatibility(
                TypeSerializer<State<TXN, CONTEXT>> newSerializer) {

            final TypeSerializerSnapshot<?>[] nestedSnapshots =
                    getNestedSerializersAndConfigs().stream()
                            .map(t -> t.f1)
                            .toArray(TypeSerializerSnapshot[]::new);

            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    newSerializer, new StateSerializerSnapshot<>(), nestedSnapshots);
        }
    }

    /** Snapshot for the {@link StateSerializer}. */
    @Internal
    public static final class StateSerializerSnapshot<TXN, CONTEXT>
            extends CompositeTypeSerializerSnapshot<
                    State<TXN, CONTEXT>, StateSerializer<TXN, CONTEXT>> {

        private static final int VERSION = 2;

        @SuppressWarnings("WeakerAccess")
        public StateSerializerSnapshot() {
            super(StateSerializer.class);
        }

        StateSerializerSnapshot(StateSerializer<TXN, CONTEXT> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected StateSerializer<TXN, CONTEXT> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            final TypeSerializer<TXN> transactionSerializer =
                    (TypeSerializer<TXN>) nestedSerializers[0];

            @SuppressWarnings("unchecked")
            final TypeSerializer<CONTEXT> contextSerializer =
                    (TypeSerializer<CONTEXT>) nestedSerializers[1];

            return new StateSerializer<>(transactionSerializer, contextSerializer);
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                StateSerializer<TXN, CONTEXT> outerSerializer) {
            return new TypeSerializer<?>[] {
                outerSerializer.transactionSerializer, outerSerializer.contextSerializer
            };
        }
    }
}
