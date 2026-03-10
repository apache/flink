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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A base class for different type delta join handler. */
public abstract class DeltaJoinHandlerBase implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable protected DeltaJoinHandlerBase next;

    protected transient OpenContext openContext;
    protected transient DeltaJoinHandlerContext handlerContext;

    public void setNext(@Nullable DeltaJoinHandlerBase next) {
        this.next = next;
    }

    public void open(OpenContext openContext, DeltaJoinHandlerContext handlerContext)
            throws Exception {
        this.openContext = openContext;
        this.handlerContext = handlerContext;

        if (next != null) {
            next.open(openContext, handlerContext);
        }
    }

    /** Start to handle this {@link DeltaJoinHandlerBase}. */
    public abstract void asyncHandle() throws Exception;

    public void completeExceptionally(Throwable error) {
        handlerContext.getRealOutputResultFuture().completeExceptionally(error);
    }

    public final DeltaJoinHandlerBase copy() {
        DeltaJoinHandlerBase newNext = null;
        if (next != null) {
            newNext = next.copy();
        }

        DeltaJoinHandlerBase newThis = this.copyInternal();

        newThis.setNext(newNext);
        return newThis;
    }

    protected abstract DeltaJoinHandlerBase copyInternal();

    public void reset() {
        if (next != null) {
            next.reset();
        }
    }

    public void close() throws Exception {
        if (next != null) {
            next.close();
        }
    }

    @VisibleForTesting
    @Nullable
    public DeltaJoinHandlerBase getNext() {
        return next;
    }

    /** A context for delta join handler. */
    public interface DeltaJoinHandlerContext {

        /** Get the buffer shared on all lookup handlers within a single delta join chain. */
        MultiInputRowDataBuffer getSharedMultiInputRowDataBuffer();

        ResultFuture<RowData> getRealOutputResultFuture();

        RuntimeContext getRuntimeContext();

        MailboxExecutor getMailboxExecutor();

        AsyncFunction<RowData, Object> getLookupFunction(int lookupOrdinal);

        boolean inLeft2RightLookupChain();
    }
}
