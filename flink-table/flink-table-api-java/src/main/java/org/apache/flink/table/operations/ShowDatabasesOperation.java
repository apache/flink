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

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operation to describe a SHOW DATABASES statement. */
public class ShowDatabasesOperation implements ShowOperation {

    private final String catalogName;
    private final boolean withLike;
    private final boolean notLike;
    private final String likePattern;
    private final String preposition;

    public ShowDatabasesOperation() {
        this.catalogName = null;
        this.likePattern = null;
        this.withLike = false;
        this.notLike = false;
        this.preposition = null;
    }

    public ShowDatabasesOperation(String likePattern, boolean withLike, boolean notLike) {
        this.catalogName = null;
        this.likePattern =
                withLike ? checkNotNull(likePattern, "Like pattern must not be null") : null;
        this.withLike = withLike;
        this.notLike = notLike;
        this.preposition = null;
    }

    public ShowDatabasesOperation(
            String catalogName,
            String likePattern,
            boolean withLike,
            boolean notLike,
            String preposition) {
        this.catalogName = checkNotNull(catalogName, "Catalog name must not be null");
        this.likePattern =
                withLike ? checkNotNull(likePattern, "Like pattern must not be null") : null;
        this.withLike = withLike;
        this.notLike = notLike;
        this.preposition = checkNotNull(preposition, "Preposition must not be null");
    }

    public String getLikePattern() {
        return likePattern;
    }

    public String getPreposition() {
        return preposition;
    }

    public boolean isWithLike() {
        return withLike;
    }

    public boolean isNotLike() {
        return notLike;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder().append("SHOW DATABASES");
        if (this.preposition != null) {
            builder.append(String.format(" %s %s", preposition, catalogName));
        }
        if (this.withLike) {
            if (notLike) {
                builder.append(String.format(" %s LIKE %s", "NOT", likePattern));
            } else {
                builder.append(String.format(" LIKE %s", likePattern));
            }
        }
        return builder.toString();
    }
}
