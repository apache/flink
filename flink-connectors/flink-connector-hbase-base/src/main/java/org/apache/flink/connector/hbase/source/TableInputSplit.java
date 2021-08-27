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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.LocatableInputSplit;

/**
 * This class implements a input splits for HBase. Each table input split corresponds to a key range
 * (low, high). All references to row below refer to the key of the row.
 */
@Internal
public class TableInputSplit extends LocatableInputSplit {

    private static final long serialVersionUID = 1L;

    /** The name of the table to retrieve data from. */
    private final byte[] tableName;

    /** The start row of the split. */
    private final byte[] startRow;

    /** The end row of the split. */
    private final byte[] endRow;

    /**
     * Creates a new table input split.
     *
     * @param splitNumber the number of the input split
     * @param hostnames the names of the hosts storing the data the input split refers to
     * @param tableName the name of the table to retrieve data from
     * @param startRow the start row of the split
     * @param endRow the end row of the split
     */
    public TableInputSplit(
            final int splitNumber,
            final String[] hostnames,
            final byte[] tableName,
            final byte[] startRow,
            final byte[] endRow) {
        super(splitNumber, hostnames);

        this.tableName = tableName;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    /**
     * Returns the table name.
     *
     * @return The table name.
     */
    public byte[] getTableName() {
        return this.tableName;
    }

    /**
     * Returns the start row.
     *
     * @return The start row.
     */
    public byte[] getStartRow() {
        return this.startRow;
    }

    /**
     * Returns the end row.
     *
     * @return The end row.
     */
    public byte[] getEndRow() {
        return this.endRow;
    }
}
