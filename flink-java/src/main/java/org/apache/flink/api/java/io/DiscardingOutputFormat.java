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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

/**
 * An output format that simply discards all elements.
 *
 * @param <T> The type of the elements accepted by the output format.
 */
@Public
public class DiscardingOutputFormat<T> implements OutputFormat<T> {

    private static final long serialVersionUID = 1L;

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) {}

    @Override
    public void writeRecord(T record) {}

    @Override
    public void close() {}
}
