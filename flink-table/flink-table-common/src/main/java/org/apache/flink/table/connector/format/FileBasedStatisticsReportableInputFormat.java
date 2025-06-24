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

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * Extension of input format which is able to report estimated statistics for file based connector.
 *
 * <p>This interface is used by file-based connectors which should also implement {@link
 * SupportsStatisticReport}. Since file have different formats, and each format has a different way
 * of storing and obtaining statistics information. For example: for Parquet and Orc, they both
 * store the metadata information in the file footer, which including row count, max/min, null
 * count, etc. While, for csv, there is no other metadata information excluding file size, one
 * approach to estimate row count is: the entire file size divided by the average length of the
 * sampled rows.
 *
 * <p>Note: This method is called at plan optimization phase, the implementation of this interface
 * should be as light as possible, but more complete information.
 */
@PublicEvolving
public interface FileBasedStatisticsReportableInputFormat {

    /**
     * Returns the estimated statistics of this input format.
     *
     * @param files The files to be estimated.
     * @param producedDataType the final output type of the format.
     */
    TableStats reportStatistics(List<Path> files, DataType producedDataType);
}
