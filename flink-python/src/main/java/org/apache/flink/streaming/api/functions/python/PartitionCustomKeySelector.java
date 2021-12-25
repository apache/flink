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

package org.apache.flink.streaming.api.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

/**
 * The {@link PartitionCustomKeySelector} will return the first field of the input row value. The
 * value of the first field is the desired partition index which is computed according to user
 * defined partitioner and keySelector function.
 */
@Internal
public class PartitionCustomKeySelector implements KeySelector<Row, Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public Integer getKey(Row value) throws Exception {
        return (Integer) value.getField(0);
    }
}
