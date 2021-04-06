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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts an {@link RelNode} to string with only the information from the RelNode itself without
 * the information from its inputs. This is mainly used to generate {@link
 * FlinkRelNode#getRelDetailedDescription()}.
 */
public class RelDescriptionWriterImpl implements RelWriter {

    /**
     * all the supported prefixes of RelNode class name (i.e. all the implementation of {@link
     * FlinkRelNode}).
     */
    private static final String[] REL_TYPE_NAME_PREFIXES = {
        "StreamExec", "BatchExec", "BatchPhysical", "StreamPhysical", "FlinkLogical"
    };

    private final PrintWriter pw;
    private final List<Pair<String, Object>> values = new ArrayList<>();

    public RelDescriptionWriterImpl(PrintWriter pw) {
        this.pw = pw;
    }

    @Override
    public void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        StringBuilder s = new StringBuilder();
        s.append(getNodeTypeName(rel));
        int j = 0;
        for (Pair<String, Object> value : valueList) {
            if (j++ == 0) {
                s.append("(");
            } else {
                s.append(", ");
            }
            s.append(value.getKey()).append("=[").append(value.getValue()).append("]");
        }
        if (j > 0) {
            s.append(")");
        }
        pw.print(s.toString());
    }

    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    }

    @Override
    public RelWriter input(String term, RelNode input) {
        // input nodes do not need to be displayed
        return this;
    }

    @Override
    public RelWriter item(String term, Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    @Override
    public RelWriter done(RelNode node) {
        final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf(values);
        values.clear();
        explain(node, valuesCopy);
        pw.flush();
        return this;
    }

    private String getNodeTypeName(RelNode rel) {
        String typeName = rel.getRelTypeName();
        for (String prefix : REL_TYPE_NAME_PREFIXES) {
            if (typeName.startsWith(prefix)) {
                return typeName.substring(prefix.length());
            }
        }
        throw new IllegalStateException("Unsupported RelNode class name '" + typeName + "'");
    }
}
