/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.operator.batch;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.AlgoOperator;
import org.apache.flink.ml.operator.batch.source.TableSourceBatchOp;
import org.apache.flink.table.api.Table;

/**
 * Base class of batch algorithm operators.
 *
 * <p>This class extends {@link AlgoOperator} to support data transmission between BatchOperators.
 */
public abstract class BatchOperator<T extends BatchOperator<T>> extends AlgoOperator<T> {

    public BatchOperator() {
        super();
    }

    /**
     * The constructor of BatchOperator with {@link Params}.
     *
     * @param params the initial Params.
     */
    public BatchOperator(Params params) {
        super(params);
    }

    /**
     * Link to another {@link BatchOperator}.
     *
     * <p>Link the <code>next</code> BatchOperator using this BatchOperator as its input.
     *
     * <p>For example:
     *
     * <pre>{@code
     * BatchOperator a = ...;
     * BatchOperator b = ...;
     * BatchOperator c = a.link(b)
     * }</pre>
     *
     * <p>The BatchOperator <code>c</code> in the above code is the same instance as <code>b</code>
     * which takes <code>a</code> as its input. Note that BatchOperator <code>b</code> will be
     * changed to link from BatchOperator <code>a</code>.
     *
     * @param next The operator that will be modified to add this operator to its input.
     * @param <B> type of BatchOperator returned
     * @return the linked next
     * @see #linkFrom(BatchOperator[])
     */
    public <B extends BatchOperator<?>> B link(B next) {
        next.linkFrom(this);
        return next;
    }

    /**
     * Link from others {@link BatchOperator}.
     *
     * <p>Link this object to BatchOperator using the BatchOperators as its input.
     *
     * <p>For example:
     *
     * <pre>{@code
     * BatchOperator a = ...;
     * BatchOperator b = ...;
     * BatchOperator c = ...;
     *
     * BatchOperator d = c.linkFrom(a, b)
     * }</pre>
     *
     * <p>The <code>d</code> in the above code is the same instance as BatchOperator <code>c</code>
     * which takes both <code>a</code> and <code>b</code> as its input.
     *
     * <p>note: It is not recommended to linkFrom itself or linkFrom the same group inputs twice.
     *
     * @param inputs the linked inputs
     * @return the linked this object
     */
    public abstract T linkFrom(BatchOperator<?>... inputs);

    /**
     * create a new BatchOperator from table.
     *
     * @param table the input table
     * @return the new BatchOperator
     */
    public static BatchOperator<?> fromTable(Table table) {
        return new TableSourceBatchOp(table);
    }

    protected static BatchOperator<?> checkAndGetFirst(BatchOperator<?>... inputs) {
        checkOpSize(1, inputs);
        return inputs[0];
    }
}
