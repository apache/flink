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

package org.apache.flink.table.api.bridge.java.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.internal.StatementSetImpl;

/** Implementation for {@link StreamStatementSet}. */
@Internal
public class StreamStatementSetImpl extends StatementSetImpl<StreamTableEnvironmentImpl>
        implements StreamStatementSet {

    protected StreamStatementSetImpl(StreamTableEnvironmentImpl tableEnvironment) {
        super(tableEnvironment);
    }

    @Override
    public StreamStatementSet addInsertSql(String statement) {
        return (StreamStatementSet) super.addInsertSql(statement);
    }

    @Override
    public StreamStatementSet addInsert(String targetPath, Table table) {
        return (StreamStatementSet) super.addInsert(targetPath, table);
    }

    @Override
    public StreamStatementSet addInsert(String targetPath, Table table, boolean overwrite) {
        return (StreamStatementSet) super.addInsert(targetPath, table, overwrite);
    }

    @Override
    public StreamStatementSet addInsert(TableDescriptor targetDescriptor, Table table) {
        return (StreamStatementSet) super.addInsert(targetDescriptor, table);
    }

    @Override
    public StreamStatementSet addInsert(
            TableDescriptor targetDescriptor, Table table, boolean overwrite) {
        return (StreamStatementSet) super.addInsert(targetDescriptor, table, overwrite);
    }

    @Override
    public void attachAsDataStream() {
        try {
            tableEnvironment.attachAsDataStream(operations);
        } finally {
            operations.clear();
        }
    }
}
