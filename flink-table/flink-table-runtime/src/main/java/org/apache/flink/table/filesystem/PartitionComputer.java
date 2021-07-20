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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Compute partition path from record and project non-partition columns for output writer.
 *
 * <p>See {@link RowPartitionComputer}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public interface PartitionComputer<T> extends Serializable {

    /**
     * Compute partition values from record.
     *
     * @param in input record.
     * @return partition values.
     */
    LinkedHashMap<String, String> generatePartValues(T in) throws Exception;

    /**
     * Project non-partition columns for output writer.
     *
     * @param in input record.
     * @return projected record.
     */
    T projectColumnsToWrite(T in) throws Exception;
}
