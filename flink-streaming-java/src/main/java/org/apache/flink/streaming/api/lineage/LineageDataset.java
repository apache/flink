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

import java.util.Map;

/** Lineage dataset represents the source or sink in the job. */
@PublicEvolving
public interface LineageDataset {
    /* Name for this particular dataset. */
    String name();

    /* Unique name for this dataset's storage, for example, url for jdbc connector and location for lakehouse connector. */
    String namespace();

    /* Facets for the lineage vertex to describe the particular information of dataset, such as schema and config. */
    Map<String, LineageDatasetFacet> facets();
}
