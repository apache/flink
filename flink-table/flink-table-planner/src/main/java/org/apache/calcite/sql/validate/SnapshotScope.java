/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.validate;

import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Represents the name-resolution context for expressions in an FOR SYSTEM_TIME AS OF TIMESTAMP
 * clause.
 */
public class SnapshotScope extends DelegatingScope {
    private final SqlValidatorWithSnapshot sqlValidatorWithSnapshot;

    public SnapshotScope(SqlValidatorScope parent, SchemaVersion schemaVersion) {
        super(parent);
        this.sqlValidatorWithSnapshot =
                new SqlValidatorWithSnapshot(
                        (SqlValidatorImpl) parent.getValidator(), schemaVersion);
    }

    @Override
    public void resolveTable(
            List<String> names, SqlNameMatcher nameMatcher, Path path, Resolved resolved) {
        // In the time travel case, the parent of the SnapshotScope will always be CatalogScope
        // with an EmptyScope as the parent of the CatalogScope, so we use EmptyScope here directly.
        new EmptyScope(sqlValidatorWithSnapshot).resolveTable(names, nameMatcher, path, resolved);
    }

    @Override
    public void addChild(SqlValidatorNamespace ns, String alias, boolean nullable) {
        parent.addChild(ns, alias, nullable);
    }

    @Override
    public SqlNode getNode() {
        return parent.getNode();
    }

    @Override
    public boolean isWithin(@Nullable SqlValidatorScope scope2) {
        return parent.isWithin(scope2);
    }
}
