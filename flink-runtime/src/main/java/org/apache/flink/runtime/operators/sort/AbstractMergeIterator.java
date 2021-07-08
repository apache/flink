/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.resettable.NonReusingBlockResettableIterator;
import org.apache.flink.runtime.operators.resettable.SpillingResettableIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.runtime.util.KeyGroupedIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractMergeIterator<T1, T2, O> implements JoinTaskIterator<T1, T2, O> {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    protected TypePairComparator<T1, T2> pairComparator;

    protected KeyGroupedIterator<T1> iterator1;
    protected KeyGroupedIterator<T2> iterator2;

    protected final TypeSerializer<T1> serializer1;
    protected final TypeSerializer<T2> serializer2;

    private final NonReusingBlockResettableIterator<T2>
            blockIt; // for N:M cross products with same key

    private final IOManager ioManager;
    private final MemoryManager memoryManager;
    private final List<MemorySegment> memoryForSpillingIterator;

    // instances for object reuse
    protected T1 copy1;
    protected T1 spillHeadCopy;
    protected T2 copy2;
    protected T2 blockHeadCopy;

    public AbstractMergeIterator(
            MutableObjectIterator<T1> input1,
            MutableObjectIterator<T2> input2,
            TypeSerializer<T1> serializer1,
            TypeComparator<T1> comparator1,
            TypeSerializer<T2> serializer2,
            TypeComparator<T2> comparator2,
            TypePairComparator<T1, T2> pairComparator,
            MemoryManager memoryManager,
            IOManager ioManager,
            int numMemoryPages,
            AbstractInvokable parentTask)
            throws MemoryAllocationException {
        if (numMemoryPages < 2) {
            throw new IllegalArgumentException("Merger needs at least 2 memory pages.");
        }

        this.pairComparator = pairComparator;
        this.serializer1 = serializer1;
        this.serializer2 = serializer2;

        this.memoryManager = memoryManager;
        this.ioManager = ioManager;

        this.iterator1 = createKeyGroupedIterator(input1, serializer1, comparator1.duplicate());
        this.iterator2 = createKeyGroupedIterator(input2, serializer2, comparator2.duplicate());

        final int numPagesForSpiller = numMemoryPages > 20 ? 2 : 1;
        this.blockIt =
                new NonReusingBlockResettableIterator<>(
                        this.memoryManager,
                        this.serializer2,
                        (numMemoryPages - numPagesForSpiller),
                        parentTask);
        this.memoryForSpillingIterator =
                memoryManager.allocatePages(parentTask, numPagesForSpiller);
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() {
        if (this.blockIt != null) {
            try {
                this.blockIt.close();
            } catch (Throwable t) {
                LOG.error("Error closing block memory iterator: " + t.getMessage(), t);
            }
        }

        this.memoryManager.release(this.memoryForSpillingIterator);
    }

    @Override
    public void abort() {
        close();
    }

    /**
     * Calls the <code>JoinFunction#join()</code> method for all two key-value pairs that share the
     * same key and come from different inputs. The output of the <code>join()</code> method is
     * forwarded.
     *
     * <p>This method first zig-zags between the two sorted inputs in order to find a common key,
     * and then calls the join stub with the cross product of the values.
     *
     * @throws Exception Forwards all exceptions from the user code and the I/O system.
     * @see
     *     org.apache.flink.runtime.operators.util.JoinTaskIterator#callWithNextKey(org.apache.flink.api.common.functions.FlatJoinFunction,
     *     org.apache.flink.util.Collector)
     */
    @Override
    public abstract boolean callWithNextKey(
            final FlatJoinFunction<T1, T2, O> joinFunction, final Collector<O> collector)
            throws Exception;

    protected void crossMatchingGroup(
            Iterator<T1> values1,
            Iterator<T2> values2,
            FlatJoinFunction<T1, T2, O> joinFunction,
            Collector<O> collector)
            throws Exception {
        final T1 firstV1 = values1.next();
        final T2 firstV2 = values2.next();

        final boolean v1HasNext = values1.hasNext();
        final boolean v2HasNext = values2.hasNext();

        // check if one side is already empty
        // this check could be omitted if we put this in MatchTask.
        // then we can derive the local strategy (with build side).

        if (v1HasNext) {
            if (v2HasNext) {
                // both sides contain more than one value
                // TODO: Decide which side to spill and which to block!
                crossMwithNValues(firstV1, values1, firstV2, values2, joinFunction, collector);
            } else {
                crossSecond1withNValues(firstV2, firstV1, values1, joinFunction, collector);
            }
        } else {
            if (v2HasNext) {
                crossFirst1withNValues(firstV1, firstV2, values2, joinFunction, collector);
            } else {
                // both sides contain only one value
                joinFunction.join(firstV1, firstV2, collector);
            }
        }
    }

    /**
     * Crosses a single value from the first input with N values, all sharing a common key.
     * Effectively realizes a <i>1:N</i> join.
     *
     * @param val1 The value form the <i>1</i> side.
     * @param firstValN The first of the values from the <i>N</i> side.
     * @param valsN Iterator over remaining <i>N</i> side values.
     * @throws Exception Forwards all exceptions thrown by the stub.
     */
    private void crossFirst1withNValues(
            final T1 val1,
            final T2 firstValN,
            final Iterator<T2> valsN,
            final FlatJoinFunction<T1, T2, O> joinFunction,
            final Collector<O> collector)
            throws Exception {
        T1 copy1 = createCopy(serializer1, val1, this.copy1);
        joinFunction.join(copy1, firstValN, collector);

        // set copy and join first element
        boolean more = true;
        do {
            final T2 nRec = valsN.next();

            if (valsN.hasNext()) {
                copy1 = createCopy(serializer1, val1, this.copy1);
                joinFunction.join(copy1, nRec, collector);
            } else {
                joinFunction.join(val1, nRec, collector);
                more = false;
            }
        } while (more);
    }

    /**
     * Crosses a single value from the second side with N values, all sharing a common key.
     * Effectively realizes a <i>N:1</i> join.
     *
     * @param val1 The value form the <i>1</i> side.
     * @param firstValN The first of the values from the <i>N</i> side.
     * @param valsN Iterator over remaining <i>N</i> side values.
     * @throws Exception Forwards all exceptions thrown by the stub.
     */
    private void crossSecond1withNValues(
            T2 val1,
            T1 firstValN,
            Iterator<T1> valsN,
            FlatJoinFunction<T1, T2, O> joinFunction,
            Collector<O> collector)
            throws Exception {
        T2 copy2 = createCopy(serializer2, val1, this.copy2);
        joinFunction.join(firstValN, copy2, collector);

        // set copy and join first element
        boolean more = true;
        do {
            final T1 nRec = valsN.next();

            if (valsN.hasNext()) {
                copy2 = createCopy(serializer2, val1, this.copy2);
                joinFunction.join(nRec, copy2, collector);
            } else {
                joinFunction.join(nRec, val1, collector);
                more = false;
            }
        } while (more);
    }

    private void crossMwithNValues(
            final T1 firstV1,
            Iterator<T1> spillVals,
            final T2 firstV2,
            final Iterator<T2> blockVals,
            final FlatJoinFunction<T1, T2, O> joinFunction,
            final Collector<O> collector)
            throws Exception {
        // ==================================================
        // We have one first (head) element from both inputs (firstV1 and firstV2)
        // We have an iterator for both inputs.
        // we make the V1 side the spilling side and the V2 side the blocking side.
        // In order to get the full cross product without unnecessary spilling, we do the
        // following:
        // 1) cross the heads
        // 2) cross the head of the spilling side against the first block of the blocking side
        // 3) cross the iterator of the spilling side with the head of the block side
        // 4) cross the iterator of the spilling side with the first block
        // ---------------------------------------------------
        // If the blocking side has more than one block, we really need to make the spilling side
        // fully
        // resettable. For each further block on the block side, we do:
        // 5) cross the head of the spilling side with the next block
        // 6) cross the spilling iterator with the next block.

        // join the first values first
        T1 copy1 = this.createCopy(serializer1, firstV1, this.copy1);
        T2 blockHeadCopy = this.createCopy(serializer2, firstV2, this.blockHeadCopy);
        T1 spillHeadCopy = null;

        // --------------- 1) Cross the heads -------------------
        joinFunction.join(copy1, firstV2, collector);

        // for the remaining values, we do a block-nested-loops join
        SpillingResettableIterator<T1> spillIt = null;

        try {
            // create block iterator on the second input
            this.blockIt.reopen(blockVals);

            // ------------- 2) cross the head of the spilling side with the first block
            // ------------------
            while (this.blockIt.hasNext()) {
                final T2 nextBlockRec = this.blockIt.next();
                copy1 = this.createCopy(serializer1, firstV1, this.copy1);
                joinFunction.join(copy1, nextBlockRec, collector);
            }
            this.blockIt.reset();

            // spilling is required if the blocked input has data beyond the current block.
            // in that case, create the spilling iterator
            final Iterator<T1> leftSideIter;
            final boolean spillingRequired = this.blockIt.hasFurtherInput();
            if (spillingRequired) {
                // more data than would fit into one block. we need to wrap the other side in a
                // spilling iterator
                // create spilling iterator on first input
                spillIt =
                        new SpillingResettableIterator<>(
                                spillVals,
                                this.serializer1,
                                this.memoryManager,
                                this.ioManager,
                                this.memoryForSpillingIterator);
                leftSideIter = spillIt;
                spillIt.open();

                spillHeadCopy = this.createCopy(serializer1, firstV1, this.spillHeadCopy);
            } else {
                leftSideIter = spillVals;
            }

            // cross the values in the v1 iterator against the current block

            while (leftSideIter.hasNext()) {
                final T1 nextSpillVal = leftSideIter.next();
                copy1 = this.createCopy(serializer1, nextSpillVal, this.copy1);

                // -------- 3) cross the iterator of the spilling side with the head of the block
                // side --------
                T2 copy2 = this.createCopy(serializer2, blockHeadCopy, this.copy2);
                joinFunction.join(copy1, copy2, collector);

                // -------- 4) cross the iterator of the spilling side with the first block --------
                while (this.blockIt.hasNext()) {
                    T2 nextBlockRec = this.blockIt.next();

                    // get instances of key and block value
                    copy1 = this.createCopy(serializer1, nextSpillVal, this.copy1);
                    joinFunction.join(copy1, nextBlockRec, collector);
                }
                // reset block iterator
                this.blockIt.reset();
            }

            // if everything from the block-side fit into a single block, we are done.
            // note that in this special case, we did not create a spilling iterator at all
            if (!spillingRequired) {
                return;
            }

            // here we are, because we have more blocks on the block side
            // loop as long as there are blocks from the blocked input
            while (this.blockIt.nextBlock()) {
                // rewind the spilling iterator
                spillIt.reset();

                // ------------- 5) cross the head of the spilling side with the next block
                // ------------
                while (this.blockIt.hasNext()) {
                    copy1 = this.createCopy(serializer1, spillHeadCopy, this.copy1);
                    final T2 nextBlockVal = blockIt.next();
                    joinFunction.join(copy1, nextBlockVal, collector);
                }
                this.blockIt.reset();

                // -------- 6) cross the spilling iterator with the next block. ------------------
                while (spillIt.hasNext()) {
                    // get value from resettable iterator
                    final T1 nextSpillVal = spillIt.next();
                    // cross value with block values
                    while (this.blockIt.hasNext()) {
                        // get instances of key and block value
                        final T2 nextBlockVal = this.blockIt.next();
                        copy1 = this.createCopy(serializer1, nextSpillVal, this.copy1);
                        joinFunction.join(copy1, nextBlockVal, collector);
                    }

                    // reset block iterator
                    this.blockIt.reset();
                }
                // reset v1 iterator
                spillIt.reset();
            }
        } finally {
            if (spillIt != null) {
                this.memoryForSpillingIterator.addAll(spillIt.close());
            }
        }
    }

    protected abstract <T> KeyGroupedIterator<T> createKeyGroupedIterator(
            MutableObjectIterator<T> input,
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator);

    /**
     * Copies an instance of the given type, potentially reusing the object passed as the reuse
     * parameter, which may be null.
     */
    protected abstract <T> T createCopy(TypeSerializer<T> serializer, T value, T reuse);
}
