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

package org.apache.flink.table.gateway.api.results;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** The collection of the results. */
@PublicEvolving
public class ResultSet implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_COLUMN_INFOS = "columns";

    public static final String FIELD_NAME_DATA = "data";

    private final ResultType resultType;

    @Nullable private final Long nextToken;

    private final ResolvedSchema resultSchema;
    private final List<RowData> data;

    public static final ResultSet NOT_READY_RESULTS =
            new ResultSet(
                    ResultType.NOT_READY,
                    0L,
                    ResolvedSchema.of(Collections.emptyList()),
                    Collections.emptyList());

    public ResultSet(
            ResultType resultType,
            @Nullable Long nextToken,
            ResolvedSchema resultSchema,
            List<RowData> data) {
        this.nextToken = nextToken;
        this.resultType = resultType;
        this.resultSchema = resultSchema;
        this.data = data;
    }

    /** Get the type of the results, which may indicate the result is EOS or has data. */
    public ResultType getResultType() {
        return resultType;
    }

    /**
     * The token indicates the next batch of the data.
     *
     * <p>When the token is null, it means all the data has been fetched.
     */
    public @Nullable Long getNextToken() {
        return nextToken;
    }

    /**
     * The schema of the data.
     *
     * <p>The schema of the DDL, USE, EXPLAIN, SHOW and DESCRIBE align with the schema of the {@link
     * TableResult#getResolvedSchema()}. The only differences is the schema of the `INSERT`
     * statement.
     *
     * <p>The schema of INSERT:
     *
     * <pre>
     * +-------------+-------------+----------+
     * | column name | column type | comments |
     * +-------------+-------------+----------+
     * |   job id    |    string   |          |
     * +- -----------+-------------+----------+
     * </pre>
     */
    public ResolvedSchema getResultSchema() {
        return resultSchema;
    }

    /** All the data in the current results. */
    public List<RowData> getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format(
                "ResultSet{\n"
                        + "  resultType=%s,\n"
                        + "  nextToken=%s,\n"
                        + "  resultSchema=%s,\n"
                        + "  data=[%s]\n"
                        + "}",
                resultType,
                nextToken,
                resultSchema.toString(),
                data.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResultSet)) {
            return false;
        }
        ResultSet resultSet = (ResultSet) o;
        return resultType == resultSet.resultType
                && Objects.equals(nextToken, resultSet.nextToken)
                && Objects.equals(resultSchema, resultSet.resultSchema)
                && Objects.equals(data, resultSet.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultType, nextToken, resultSchema, data);
    }

    /** Describe the kind of the result. */
    @PublicEvolving
    public enum ResultType {
        /** Indicate the result is not ready. */
        NOT_READY,

        /** Indicate the result has data. */
        PAYLOAD,

        /** Indicate all results have been fetched. */
        EOS
    }
}
