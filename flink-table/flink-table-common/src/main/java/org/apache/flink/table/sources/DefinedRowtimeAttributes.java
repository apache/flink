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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;

import java.util.List;

/**
 * Extends a {@link TableSource} to specify rowtime attributes via a {@link
 * RowtimeAttributeDescriptor}.
 *
 * @deprecated This interface will not be supported in the new source design around {@link
 *     DynamicTableSource}. Use the concept of computed columns instead. See FLIP-95 for more
 *     information.
 */
@Deprecated
@PublicEvolving
public interface DefinedRowtimeAttributes {

    /**
     * Returns a list of {@link RowtimeAttributeDescriptor} for all rowtime attributes of the table.
     *
     * <p>All referenced attributes must be present in the {@link TableSchema} of the {@link
     * TableSource} and of type {@link Types#SQL_TIMESTAMP}.
     *
     * @return A list of {@link RowtimeAttributeDescriptor}.
     */
    List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors();
}
