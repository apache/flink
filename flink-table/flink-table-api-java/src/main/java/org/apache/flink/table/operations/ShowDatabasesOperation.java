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
import org.apache.flink.table.functions.SqlLikeUtils;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW DATABASES statement. */
@Internal
public class ShowDatabasesOperation implements ShowOperation {

    private final String catalogName;
    private final LikeType likeType;
    private final String likePattern;
    private final boolean notLike;

    public ShowDatabasesOperation() {
        // "SHOW DATABASES" command with all options being default
        this(null, null, null, false);
    }

    public ShowDatabasesOperation(String likeType, String likePattern, boolean notLike) {
        this(null, likeType, likePattern, notLike);
    }

    public ShowDatabasesOperation(
            String catalogName, String likeType, String likePattern, boolean notLike) {
        this.catalogName = catalogName;
        if (likeType != null) {
            this.likeType = LikeType.of(likeType);
            this.likePattern = requireNonNull(likePattern, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.likePattern = null;
            this.notLike = false;
        }
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW DATABASES");
        if (catalogName != null) {
            builder.append(String.format(" FROM/IN %s", catalogName));
        }
        if (likeType != null) {
            if (notLike) {
                builder.append(String.format(" NOT %s '%s'", likeType.name(), likePattern));
            } else {
                builder.append(String.format(" %s '%s'", likeType.name(), likePattern));
            }
        }
        return builder.toString();
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        String cName =
                catalogName == null ? ctx.getCatalogManager().getCurrentCatalog() : catalogName;
        Stream<String> databases =
                ctx.getCatalogManager().getCatalogOrThrowException(cName).listDatabases().stream();

        if (likeType != null) {
            databases =
                    databases.filter(
                            row -> {
                                if (likeType == LikeType.ILIKE) {
                                    return notLike != SqlLikeUtils.ilike(row, likePattern, "\\");
                                } else if (likeType == LikeType.LIKE) {
                                    return notLike != SqlLikeUtils.like(row, likePattern, "\\");
                                }
                                return false;
                            });
        }

        return buildStringArrayResult("database name", databases.sorted().toArray(String[]::new));
    }
}
