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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Experimental;

/**
 * Enricher interface for determining materialized table properties during catalog resolution.
 *
 * <p>This enricher resolves:
 *
 * <ul>
 *   <li>Freshness intervals when not explicitly specified by the user
 *   <li>Physical refresh modes (CONTINUOUS or FULL) based on logical preferences and configuration
 * </ul>
 *
 * <p>Implementations can provide custom strategies tailored to different deployment environments or
 * operational requirements.
 */
@Experimental
public interface MaterializedTableEnricher {

    /**
     * Enriches a materialized table by determining its final freshness interval and refresh mode.
     *
     * @param catalogMaterializedTable the materialized table to enrich, which may have null
     *     freshness
     * @return the enrichment result with resolved, non-null freshness and refresh mode
     */
    MaterializedTableEnrichmentResult enrich(CatalogMaterializedTable catalogMaterializedTable);
}
