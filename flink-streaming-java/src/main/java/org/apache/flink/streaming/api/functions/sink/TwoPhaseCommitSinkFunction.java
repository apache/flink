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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This is a recommended base class for all of the {@link SinkFunction} that intend to implement exactly-once semantic.
 * It does that by implementing two phase commit algorithm on top of the {@link CheckpointedFunction} and
 * {@link CheckpointListener}. User should provide custom {@code TXN} (transaction handle) and implement abstract
 * methods handling this transaction handle.
 *
 * @param <IN> Input type for {@link SinkFunction}.
 * @param <TXN> Transaction to store all of the information required to handle a transaction.
 * @param <CONTEXT> Context that will be shared across all invocations for the given {@link TwoPhaseCommitSinkFunction}
 *                 instance. Context is created once
 */
@PublicEvolving
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener {

	private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitSinkFunction.class);

	protected final ListStateDescriptor<State<TXN, CONTEXT>> stateDescriptor;

	protected final LinkedHashMap<Long, TXN> pendingCommitTransactions = new LinkedHashMap<>();

	@Nullable
	protected TXN currentTransaction;
	protected Optional<CONTEXT> userContext;

	protected ListState<State<TXN, CONTEXT>> state;

	/**
	 * Use default {@link ListStateDescriptor} for internal state serialization. Helpful utilities for using this
	 * constructor are {@link TypeInformation#of(Class)}, {@link org.apache.flink.api.common.typeinfo.TypeHint} and
	 * {@link TypeInformation#of(TypeHint)}. Example:
	 * <pre>
	 * {@code
	 * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
	 * }
	 * </pre>
	 * @param stateTypeInformation {@link TypeInformation} for POJO holding state of opened transactions.
	 */
	public TwoPhaseCommitSinkFunction(TypeInformation<State<TXN, CONTEXT>> stateTypeInformation) {
		this(new ListStateDescriptor<State<TXN, CONTEXT>>("state", stateTypeInformation));
	}

	/**
	 * Instantiate {@link TwoPhaseCommitSinkFunction} with custom state descriptors.
	 *
	 * @param stateDescriptor descriptor for transactions POJO.
	 */
	public TwoPhaseCommitSinkFunction(ListStateDescriptor<State<TXN, CONTEXT>> stateDescriptor) {
		this.stateDescriptor = requireNonNull(stateDescriptor, "stateDescriptor is null");
	}

	protected Optional<CONTEXT> initializeUserContext() {
		return Optional.empty();
	}

	protected Optional<CONTEXT> getUserContext() {
		return userContext;
	}

	// ------ methods that should be implemented in child class to support two phase commit algorithm ------

	/**
	 * Write value within a transaction.
	 */
	protected abstract void invoke(TXN transaction, IN value) throws Exception;

	/**
	 * Method that starts a new transaction.
	 *
	 * @return newly created transaction.
	 */
	protected abstract TXN beginTransaction() throws Exception;

	/**
	 * Pre commit previously created transaction. Pre commit must make all of the necessary steps to prepare the
	 * transaction for a commit that might happen in the future. After this point the transaction might still be
	 * aborted, but underlying implementation must ensure that commit calls on already pre committed transactions
	 * will always succeed.
	 *
	 * <p>Usually implementation involves flushing the data.
	 */
	protected abstract void preCommit(TXN transaction) throws Exception;

	/**
	 * Commit a pre-committed transaction. If this method fail, Flink application will be
	 * restarted and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
	 * same transaction.
	 */
	protected abstract void commit(TXN transaction);

	/**
	 * Invoked on recovered transactions after a failure. User implementation must ensure that this call will eventually
	 * succeed. If it fails, Flink application will be restarted and it will be invoked again. If it does not succeed
	 * a data loss will occur. Transactions will be recovered in an order in which they were created.
	 */
	protected void recoverAndCommit(TXN transaction) {
		commit(transaction);
	}

	/**
	 * Abort a transaction.
	 */
	protected abstract void abort(TXN transaction);

	/**
	 * Abort a transaction that was rejected by a coordinator after a failure.
	 */
	protected void recoverAndAbort(TXN transaction) {
		abort(transaction);
	}

	protected void finishRecoveringContext() {
	}

	// ------ entry points for above methods implementing {@CheckPointedFunction} and {@CheckpointListener} ------

	@Override
	public final void invoke(IN value) throws Exception {
		invoke(currentTransaction, value);
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

		Iterator<Map.Entry<Long, TXN>> pendingTransactionIterator = pendingCommitTransactions.entrySet().iterator();
		checkState(pendingTransactionIterator.hasNext(), "checkpoint completed, but no transaction pending");

		while (pendingTransactionIterator.hasNext()) {
			Map.Entry<Long, TXN> entry = pendingTransactionIterator.next();
			Long pendingTransactionCheckpointId = entry.getKey();
			TXN pendingTransaction = entry.getValue();
			if (pendingTransactionCheckpointId > checkpointId) {
				continue;
			}

			LOG.info("{} - checkpoint {} complete, committing transaction {} from checkpoint {}",
				name(), checkpointId, pendingTransaction, pendingTransactionCheckpointId);

			commit(pendingTransaction);

			LOG.debug("{} - committed checkpoint transaction {}", name(), pendingTransaction);

			pendingTransactionIterator.remove();
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// this is like the pre-commit of a 2-phase-commit transaction
		// we are ready to commit and remember the transaction

		checkState(currentTransaction != null, "bug: no transaction object when performing state snapshot");

		long checkpointId = context.getCheckpointId();
		LOG.debug("{} - checkpoint {} triggered, flushing transaction '{}'", name(), context.getCheckpointId(), currentTransaction);

		preCommit(currentTransaction);
		pendingCommitTransactions.put(checkpointId, currentTransaction);
		LOG.debug("{} - stored pending transactions {}", name(), pendingCommitTransactions);

		currentTransaction = beginTransaction();
		LOG.debug("{} - started new transaction '{}'", name(), currentTransaction);

		state.clear();
		state.add(new State<>(
			this.currentTransaction,
			new ArrayList<>(pendingCommitTransactions.values()),
			userContext));
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// when we are restoring state with pendingCommitTransactions, we don't really know whether the
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

		if (context.isRestored()) {
			LOG.info("{} - restoring state", name());

			for (State<TXN, CONTEXT> operatorState : state.get()) {
				userContext = operatorState.getContext();
				List<TXN> recoveredTransactions = operatorState.getPendingCommitTransactions();
				for (TXN recoveredTransaction : recoveredTransactions) {
					// If this fails, there is actually a data loss
					recoverAndCommit(recoveredTransaction);
					LOG.info("{} committed recovered transaction {}", name(), recoveredTransaction);
				}

				recoverAndAbort(operatorState.getPendingTransaction());
				LOG.info("{} aborted recovered transaction {}", name(), operatorState.getPendingTransaction());

				if (userContext.isPresent()) {
					finishRecoveringContext();
				}
			}
		}
		// if in restore we didn't get any userContext or we are initializing from scratch
		if (userContext == null) {
			LOG.info("{} - no state to restore", name());

			userContext = initializeUserContext();
		}
		this.pendingCommitTransactions.clear();

		currentTransaction = beginTransaction();
		LOG.debug("{} - started new transaction '{}'", name(), currentTransaction);
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (currentTransaction != null) {
			abort(currentTransaction);
			currentTransaction = null;
		}
	}

	private String name() {
		return String.format(
			"%s %s/%s",
			this.getClass().getSimpleName(),
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks());
	}

	/**
	 * State POJO class coupling pendingTransaction, context and pendingCommitTransactions.
	 */
	public static class State<TXN, CONTEXT> {
		protected TXN pendingTransaction;
		protected List<TXN> pendingCommitTransactions = new ArrayList<>();
		protected Optional<CONTEXT> context;

		public State() {
		}

		public State(TXN pendingTransaction, List<TXN> pendingCommitTransactions, Optional<CONTEXT> context) {
			this.context = requireNonNull(context, "context is null");
			this.pendingTransaction = requireNonNull(pendingTransaction, "pendingTransaction is null");
			this.pendingCommitTransactions = requireNonNull(pendingCommitTransactions, "pendingCommitTransactions is null");
		}

		public TXN getPendingTransaction() {
			return pendingTransaction;
		}

		public void setPendingTransaction(TXN pendingTransaction) {
			this.pendingTransaction = pendingTransaction;
		}

		public List<TXN> getPendingCommitTransactions() {
			return pendingCommitTransactions;
		}

		public void setPendingCommitTransactions(List<TXN> pendingCommitTransactions) {
			this.pendingCommitTransactions = pendingCommitTransactions;
		}

		public Optional<CONTEXT> getContext() {
			return context;
		}

		public void setContext(Optional<CONTEXT> context) {
			this.context = context;
		}
	}
}
