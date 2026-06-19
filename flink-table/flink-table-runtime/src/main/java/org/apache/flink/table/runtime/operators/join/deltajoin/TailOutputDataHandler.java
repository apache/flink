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

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * A tail handler to output the data.
 *
 * <p>Note: this handler is only used in the tail of the delta join chain and should not have a
 * concrete handler after it.
 */
public class TailOutputDataHandler extends DeltaJoinHandlerBase {

    private static final long serialVersionUID = 1L;

    private final int[] allLookupSideBinaryInputOrdinals;

    public TailOutputDataHandler(int[] allLookupSideBinaryInputOrdinals) {
        this.allLookupSideBinaryInputOrdinals = allLookupSideBinaryInputOrdinals;
    }

    @Override
    public void setNext(@Nullable DeltaJoinHandlerBase next) {
        super.setNext(next);

        Preconditions.checkArgument(
                next == null, "This tail handler should not have a concrete handler after it");
    }

    @Override
    public void asyncHandle() throws Exception {
        MultiInputRowDataBuffer sharedMultiInputRowDataBuffer =
                handlerContext.getSharedMultiInputRowDataBuffer();

        Collection<RowData> allData =
                sharedMultiInputRowDataBuffer.getData(allLookupSideBinaryInputOrdinals);
        handlerContext.getRealOutputResultFuture().complete(allData);
    }

    @Override
    protected DeltaJoinHandlerBase copyInternal() {
        return new TailOutputDataHandler(allLookupSideBinaryInputOrdinals);
    }
}
