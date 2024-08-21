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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/**
 * Base class for SHOW operations. It provides support of functionality:
 *
 * <pre>
 * [ ( FROM | IN ) catalog_name] [ [NOT] (LIKE | ILIKE) &lt;sql_like_pattern&gt; ]
 * </pre>
 */
@Internal
public abstract class AbstractShowOperation implements ShowOperation {
    protected final @Nullable String catalogName;
    protected final @Nullable String preposition;
    protected final @Nullable ShowLikeOperator likeOp;

    public AbstractShowOperation(
            @Nullable String catalogName,
            @Nullable String preposition,
            @Nullable ShowLikeOperator likeOp) {
        this.catalogName = catalogName;
        this.preposition = preposition;
        this.likeOp = likeOp;
    }

    protected abstract Collection<String> retrieveDataForTableResult(Context ctx);

    protected abstract String getOperationName();

    protected abstract String getColumnName();

    @Override
    public TableResultInternal execute(Context ctx) {
        final Collection<String> views = retrieveDataForTableResult(ctx);
        final String[] rows =
                views.stream()
                        .filter(row -> ShowLikeOperator.likeFilter(row, likeOp))
                        .sorted()
                        .toArray(String[]::new);
        return buildStringArrayResult(getColumnName(), rows);
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder().append(getOperationName());
        if (preposition != null) {
            builder.append(" ").append(getPrepositionSummaryString());
        }
        if (likeOp != null) {
            builder.append(" ").append(likeOp);
        }
        return builder.toString();
    }

    protected String getPrepositionSummaryString() {
        return String.format("%s %s", preposition, catalogName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractShowOperation that = (AbstractShowOperation) o;
        return Objects.equals(catalogName, that.catalogName)
                && Objects.equals(preposition, that.preposition)
                && Objects.equals(likeOp, that.likeOp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, preposition, likeOp);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
