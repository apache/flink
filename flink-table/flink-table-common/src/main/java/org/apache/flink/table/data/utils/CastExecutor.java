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

package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;

import javax.annotation.Nullable;

/**
 * Interface to model a function that performs the casting of a value from one type to another.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
@FunctionalInterface
public interface CastExecutor<IN, OUT> {
    /**
     * Cast the input value. The output is null only and only if the input is null. The method
     * throws an exception if something goes wrong when casting.
     *
     * @param value Input value.
     */
    @Nullable
    OUT cast(@Nullable IN value) throws TableException;
}
