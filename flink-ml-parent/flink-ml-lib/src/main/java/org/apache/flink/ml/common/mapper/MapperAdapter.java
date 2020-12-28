/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

/**
 * A class that helps adapt a {@link Mapper} to a {@link MapFunction} so that the mapper can run in
 * Flink.
 */
public class MapperAdapter implements MapFunction<Row, Row> {

    private final Mapper mapper;

    /**
     * Construct a MapperAdapter with the given mapper.
     *
     * @param mapper The {@link Mapper} to adapt.
     */
    public MapperAdapter(Mapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Row map(Row row) throws Exception {
        return this.mapper.map(row);
    }
}
