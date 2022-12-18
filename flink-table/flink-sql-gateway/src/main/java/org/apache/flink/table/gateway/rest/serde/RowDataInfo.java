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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A RowDataInfo info represents a {@link RowData}. */
@Internal
public class RowDataInfo {

    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_FIELDS = "fields";

    private final String kind;

    private final List<Object> fields;

    public RowDataInfo(String kind, List<Object> fields) {
        this.kind = Preconditions.checkNotNull(kind, "kind must not be null");
        this.fields = Preconditions.checkNotNull(fields, "fields must not be null");
    }

    public static RowDataInfo toRowDataInfo(
            RowData rowData, List<RowData.FieldGetter> fieldGetters) {
        return new RowDataInfo(
                rowData.getRowKind().name(),
                fieldGetters.stream()
                        .map(fieldGetter -> fieldGetter.getFieldOrNull(rowData))
                        .collect(Collectors.toList()));
    }

    public RowData toRowData() {
        return GenericRowData.ofKind(RowKind.valueOf(kind), fields.toArray());
    }

    public String getKind() {
        return kind;
    }

    public List<Object> getFields() {
        return fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowDataInfo)) {
            return false;
        }
        RowDataInfo that = (RowDataInfo) o;
        return Objects.equals(kind, that.kind) && Objects.equals(fields, that.fields);
    }

    @Override
    public String toString() {
        return String.format(
                "DataInfo{\n  rowKind=%s,\n  fields=[%s]\n}",
                kind, fields.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
