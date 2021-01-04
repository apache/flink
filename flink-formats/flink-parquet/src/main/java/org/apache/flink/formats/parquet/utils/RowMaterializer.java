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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Row materializer for {@link RowReadSupport}. */
public class RowMaterializer extends RecordMaterializer<Row> {
    private RowConverter root;

    public RowMaterializer(MessageType messageType, TypeInformation<?> rowTypeInfo) {
        checkNotNull(messageType, "messageType");
        checkNotNull(rowTypeInfo, "rowTypeInfo");
        this.root = new RowConverter(messageType, rowTypeInfo);
    }

    @Override
    public Row getCurrentRecord() {
        return root.getCurrentRow();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
