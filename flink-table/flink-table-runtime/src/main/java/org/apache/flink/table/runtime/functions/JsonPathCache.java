/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.cache.Cache;

/** The default cache for the jsonpath {@link com.jayway.jsonpath.spi.cache.CacheProvider}. */
public class JsonPathCache implements Cache {

    private static final long DEFAULT_CACHE_MAXIMUM_SIZE = 400;

    private final org.apache.flink.shaded.guava31.com.google.common.cache.Cache<String, JsonPath>
            jsonPathCache =
                    CacheBuilder.newBuilder().maximumSize(DEFAULT_CACHE_MAXIMUM_SIZE).build();

    @Override
    public JsonPath get(String s) {
        return jsonPathCache.getIfPresent(s);
    }

    @Override
    public void put(String s, JsonPath jsonPath) {
        jsonPathCache.put(s, jsonPath);
    }
}
