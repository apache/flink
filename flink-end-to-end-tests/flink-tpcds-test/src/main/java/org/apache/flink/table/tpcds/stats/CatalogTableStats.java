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

package org.apache.flink.table.tpcds.stats;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

/**
 * Class to describe catalog table statistics. Consists of {@link CatalogTableStatistics} and {@link
 * CatalogColumnStatistics}.
 */
public class CatalogTableStats {
    private final CatalogTableStatistics catalogTableStatistics;
    private final CatalogColumnStatistics catalogColumnStatistics;

    public CatalogTableStats(
            CatalogTableStatistics catalogTableStatistics,
            CatalogColumnStatistics catalogColumnStatistics) {
        this.catalogTableStatistics = catalogTableStatistics;
        this.catalogColumnStatistics = catalogColumnStatistics;
    }

    public void register2Catalog(TableEnvironment tEnv, String table) {

        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .ifPresent(
                        catalog -> {
                            try {
                                catalog.alterTableStatistics(
                                        new ObjectPath(tEnv.getCurrentDatabase(), table),
                                        catalogTableStatistics,
                                        false);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .ifPresent(
                        catalog -> {
                            try {
                                catalog.alterTableColumnStatistics(
                                        new ObjectPath(tEnv.getCurrentDatabase(), table),
                                        catalogColumnStatistics,
                                        false);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
