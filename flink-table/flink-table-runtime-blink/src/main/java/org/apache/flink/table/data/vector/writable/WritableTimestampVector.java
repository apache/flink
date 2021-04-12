/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector.writable;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.TimestampColumnVector;

/** Writable {@link TimestampColumnVector}. */
public interface WritableTimestampVector extends WritableColumnVector, TimestampColumnVector {

    /** Set {@link TimestampData} at rowId with the provided value. */
    void setTimestamp(int rowId, TimestampData timestamp);

    /** Fill the column vector with the provided value. */
    void fill(TimestampData value);
}
