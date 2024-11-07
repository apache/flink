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

package org.apache.flink.table.runtime.operators.join.stream.utils;

import org.apache.flink.table.data.RowData;

/**
 * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The {@code
 * numOfAssociations} represents the number of associated records in the other side. It is used when
 * the record is from outer side (e.g. left side in LEFT OUTER JOIN). When the {@code
 * numOfAssociations} is ZERO, we need to send a null padding row. This is useful to avoid recompute
 * the associated numbers every time.
 *
 * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the {@code
 * numOfAssociations} will always be {@code -1}.
 */
public final class OuterRecord {
    public final RowData record;
    public final int numOfAssociations;

    public OuterRecord(RowData record, int numOfAssociations) {
        this.record = record;
        this.numOfAssociations = numOfAssociations;
    }

    public OuterRecord(RowData record) {
        this.record = record;
        // use -1 as the default number of associations
        this.numOfAssociations = -1;
    }

    public RowData getRecord() {
        return record;
    }

    public int getNumOfAssociations() {
        return numOfAssociations;
    }
}
