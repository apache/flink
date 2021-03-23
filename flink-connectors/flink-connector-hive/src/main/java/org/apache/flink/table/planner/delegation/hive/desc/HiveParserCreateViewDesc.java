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
import org.apache.hadoop.hive.ql.parse.ASTNode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Desc for create view operation. */
public class HiveParserCreateViewDesc implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String compoundName;
    private final String comment;
    private final Map<String, String> tblProps;
    private final boolean ifNotExists;
    private final boolean isAlterViewAs;
    private final ASTNode query;

    private List<FieldSchema> schema;
    private String originalText;
    private String expandedText;

    public HiveParserCreateViewDesc(
            String compoundName,
            List<FieldSchema> schema,
            String comment,
            Map<String, String> tblProps,
            boolean ifNotExists,
            boolean isAlterViewAs,
            ASTNode query) {
        this.compoundName = compoundName;
        this.schema = schema;
        this.comment = comment;
        this.tblProps = tblProps;
        this.ifNotExists = ifNotExists;
        this.isAlterViewAs = isAlterViewAs;
        this.query = query;
    }

    public String getCompoundName() {
        return compoundName;
    }

    public List<FieldSchema> getSchema() {
        return schema;
    }

    public void setSchema(List<FieldSchema> schema) {
        this.schema = schema;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getTblProps() {
        return tblProps;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public boolean isAlterViewAs() {
        return isAlterViewAs;
    }

    public String getOriginalText() {
        return originalText;
    }

    public void setOriginalText(String originalText) {
        this.originalText = originalText;
    }

    public String getExpandedText() {
        return expandedText;
    }

    public void setExpandedText(String expandedText) {
        this.expandedText = expandedText;
    }

    public ASTNode getQuery() {
        return query;
    }

    public boolean isMaterialized() {
        return false;
    }
}
