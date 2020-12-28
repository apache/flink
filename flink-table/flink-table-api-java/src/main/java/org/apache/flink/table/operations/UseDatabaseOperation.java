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

/** Operation to describe a USE [catalogName.]dataBaseName statement. */
public class UseDatabaseOperation implements UseOperation {

    private final String catalogName;
    private final String databaseName;

    public UseDatabaseOperation(String catalogName, String databaseName) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String asSummaryString() {
        return String.format("USE %s.%s", catalogName, databaseName);
    }
}
