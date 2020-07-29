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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.plan.stats.TableStats;

/**
 * Enables to give {@link ScanTableSource} the ability to report table statistics.
 *
 * <p>Statistics can be inferred from real data in real time, it is more accurate than the
 * statistics in the catalog.
 *
 * <p>After filtering push down and partition push down, the source can have more information,
 * which can help it infer more effective table statistics.
 *
 * <p>Internal: This interface is only used by Hive and Filesystem connector.
 */
@Internal
public interface SupportsStatisticsReport {

	/**
	 * Reports {@link TableStats} from old table stats.
	 */
	TableStats reportTableStatistics(TableStats oldStats);
}
