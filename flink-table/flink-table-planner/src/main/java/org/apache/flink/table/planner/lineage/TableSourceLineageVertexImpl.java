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

package org.apache.flink.table.planner.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Implementation of TableSourceLineageVertex. */
public class TableSourceLineageVertexImpl implements TableSourceLineageVertex {
    @JsonProperty private List<LineageDataset> datasets;
    @JsonProperty private Boundedness boundedness;

    public TableSourceLineageVertexImpl(List<LineageDataset> datasets, Boundedness boundedness) {
        this.datasets = datasets;
        this.boundedness = boundedness;
    }

    @Override
    public List<LineageDataset> datasets() {
        return this.datasets;
    }

    @Override
    public Boundedness boundedness() {
        return boundedness;
    }
}
