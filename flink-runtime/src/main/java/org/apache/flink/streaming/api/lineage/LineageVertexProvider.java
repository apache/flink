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
 *
 */

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Create lineage vertex for source and sink in DataStream. If the source and sink connectors in
 * datastream job implement this interface, flink will get lineage vertex and create all-to-all
 * lineages between sources and sinks by default.
 */
@PublicEvolving
public interface LineageVertexProvider {
    LineageVertex getLineageVertex();
}
