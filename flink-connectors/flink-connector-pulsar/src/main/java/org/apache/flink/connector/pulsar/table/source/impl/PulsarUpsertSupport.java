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

package org.apache.flink.connector.pulsar.table.source.impl;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/**
 * A class used to support upsert Pulsar. It decides the row kind according to upsert configuration.
 */
public class PulsarUpsertSupport implements Serializable {
    private static final long serialVersionUID = -3792051667586401939L;

    private final boolean upsertEnabled;

    public PulsarUpsertSupport(boolean upsertEnabled) {
        this.upsertEnabled = upsertEnabled;
    }

    public RowKind decideRowKind(RowData physicalValueRow) {
        final RowKind rowKind;
        if (physicalValueRow == null) {
            if (upsertEnabled) {
                rowKind = RowKind.DELETE;
            } else {
                throw new DeserializationException(
                        "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
            }
        } else {
            rowKind = physicalValueRow.getRowKind();
        }
        return rowKind;
    }
}
