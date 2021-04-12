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

package org.apache.flink.table.planner.delegation.hive.desc;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.ADDCOLS;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.ADDFILEFORMAT;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.ADDPROPS;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.ADDSERDE;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.ADDSERDEPROPS;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.ALTERLOCATION;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.RENAME;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.RENAMECOLUMN;
import static org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes.REPLACECOLS;

/** Desc for alter table. */
public class HiveParserAlterTableDesc implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AlterTableDesc.AlterTableTypes op;
    private final String compoundName;
    private final Map<String, String> partSpec;
    private final boolean expectView;
    private final Map<String, String> props;
    private final String newName;
    private final String serdeName;
    private final String newLocation;
    private final String oldColName;
    private final String newColName;
    private final String newColType;
    private final String newColComment;
    private final boolean first;
    private final String after;
    private final List<FieldSchema> newCols;
    private final boolean cascade;

    private String genericFileFormatName;

    private HiveParserAlterTableDesc(
            AlterTableDesc.AlterTableTypes op,
            String compoundName,
            Map<String, String> partSpec,
            boolean expectView,
            Map<String, String> props,
            String newName,
            String serdeName,
            String newLocation,
            String oldColName,
            String newColName,
            String newColType,
            String newColComment,
            boolean first,
            String after,
            List<FieldSchema> newCols,
            boolean cascade) {
        this.op = op;
        this.compoundName = compoundName;
        this.partSpec = partSpec;
        this.expectView = expectView;
        this.props = props;
        this.newName = newName;
        this.serdeName = serdeName;
        this.newLocation = newLocation;
        this.oldColName = oldColName;
        this.newColName = newColName;
        this.newColType = newColType;
        this.newColComment = newColComment;
        this.first = first;
        this.after = after;
        this.newCols = newCols;
        this.cascade = cascade;
    }

    public void setGenericFileFormatName(String genericFileFormatName) {
        this.genericFileFormatName = genericFileFormatName;
    }

    public String getGenericFileFormatName() {
        return genericFileFormatName;
    }

    public AlterTableDesc.AlterTableTypes getOp() {
        return op;
    }

    public String getCompoundName() {
        return compoundName;
    }

    public Map<String, String> getPartSpec() {
        return partSpec;
    }

    public boolean expectView() {
        return expectView;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public String getNewName() {
        return newName;
    }

    public String getSerdeName() {
        return serdeName;
    }

    public String getNewLocation() {
        return newLocation;
    }

    public String getOldColName() {
        return oldColName;
    }

    public String getNewColName() {
        return newColName;
    }

    public String getNewColType() {
        return newColType;
    }

    public String getNewColComment() {
        return newColComment;
    }

    public boolean isFirst() {
        return first;
    }

    public String getAfter() {
        return after;
    }

    public List<FieldSchema> getNewCols() {
        return newCols;
    }

    public boolean isCascade() {
        return cascade;
    }

    public static HiveParserAlterTableDesc alterFileFormat(
            String compoundName, Map<String, String> partSpec) {
        return new Builder()
                .op(ADDFILEFORMAT)
                .compoundName(compoundName)
                .partSpec(partSpec)
                .build();
    }

    public static HiveParserAlterTableDesc changeColumn(
            String compoundName,
            String oldColName,
            String newColName,
            String newColType,
            String newColComment,
            boolean first,
            String after,
            boolean cascade) {
        return new Builder()
                .op(RENAMECOLUMN)
                .compoundName(compoundName)
                .oldColName(oldColName)
                .newColName(newColName)
                .newColType(newColType)
                .newColComment(newColComment)
                .first(first)
                .after(after)
                .cascade(cascade)
                .build();
    }

    public static HiveParserAlterTableDesc addReplaceColumns(
            String compoundName, List<FieldSchema> newCols, boolean replace, boolean cascade) {
        return new Builder()
                .op(replace ? REPLACECOLS : ADDCOLS)
                .compoundName(compoundName)
                .newCols(newCols)
                .cascade(cascade)
                .build();
    }

    public static HiveParserAlterTableDesc rename(
            String compoundName, String newName, boolean expectView) {
        return new Builder()
                .op(RENAME)
                .compoundName(compoundName)
                .newName(newName)
                .expectView(expectView)
                .build();
    }

    public static HiveParserAlterTableDesc alterTableProps(
            String compoundName,
            Map<String, String> partSpec,
            Map<String, String> props,
            boolean expectView) {
        return new Builder()
                .op(ADDPROPS)
                .compoundName(compoundName)
                .partSpec(partSpec)
                .props(props)
                .expectView(expectView)
                .build();
    }

    public static HiveParserAlterTableDesc alterSerDe(
            String compoundName,
            Map<String, String> partSpec,
            String serdeName,
            Map<String, String> props) {
        return new Builder()
                .op(serdeName == null ? ADDSERDEPROPS : ADDSERDE)
                .compoundName(compoundName)
                .partSpec(partSpec)
                .serdeName(serdeName)
                .props(props)
                .build();
    }

    public static HiveParserAlterTableDesc alterLocation(
            String compoundName, Map<String, String> partSpec, String newLocation) {
        return new Builder()
                .op(ALTERLOCATION)
                .compoundName(compoundName)
                .partSpec(partSpec)
                .newLocation(newLocation)
                .build();
    }

    private static class Builder {
        private AlterTableDesc.AlterTableTypes op;
        private String compoundName;
        private Map<String, String> partSpec;
        private boolean expectView;
        private Map<String, String> props;
        private String newName;
        private String serdeName;
        private String newLocation;
        private String oldColName;
        private String newColName;
        private String newColType;
        private String newColComment;
        private boolean first;
        private String after;
        private List<FieldSchema> newCols;
        private boolean cascade;

        Builder op(AlterTableDesc.AlterTableTypes op) {
            this.op = op;
            return this;
        }

        Builder compoundName(String compoundName) {
            this.compoundName = compoundName;
            return this;
        }

        Builder partSpec(Map<String, String> partSpec) {
            this.partSpec = partSpec;
            return this;
        }

        Builder expectView(boolean expectView) {
            this.expectView = expectView;
            return this;
        }

        Builder props(Map<String, String> props) {
            this.props = props;
            return this;
        }

        Builder newName(String newName) {
            this.newName = newName;
            return this;
        }

        Builder serdeName(String serdeName) {
            this.serdeName = serdeName;
            return this;
        }

        Builder newLocation(String newLocation) {
            this.newLocation = newLocation;
            return this;
        }

        Builder oldColName(String oldColName) {
            this.oldColName = oldColName;
            return this;
        }

        Builder newColName(String newColName) {
            this.newColName = newColName;
            return this;
        }

        Builder newColType(String newColType) {
            this.newColType = newColType;
            return this;
        }

        Builder newColComment(String newColComment) {
            this.newColComment = newColComment;
            return this;
        }

        Builder first(boolean first) {
            this.first = first;
            return this;
        }

        Builder after(String after) {
            this.after = after;
            return this;
        }

        Builder newCols(List<FieldSchema> newCols) {
            this.newCols = newCols;
            return this;
        }

        Builder cascade(boolean cascade) {
            this.cascade = cascade;
            return this;
        }

        HiveParserAlterTableDesc build() {
            return new HiveParserAlterTableDesc(
                    op,
                    compoundName,
                    partSpec,
                    expectView,
                    props,
                    newName,
                    serdeName,
                    newLocation,
                    oldColName,
                    newColName,
                    newColType,
                    newColComment,
                    first,
                    after,
                    newCols,
                    cascade);
        }
    }
}
