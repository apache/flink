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

package org.apache.flink.table.catalog.classloader;

import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Test catalog which requires a proper context classloader when opening. */
public class TestCatalog extends GenericInMemoryCatalog {

    public TestCatalog(String name) {
        super(name);
    }

    public TestCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);
    }

    @Override
    public void open() {
        List<TestService> serviceList =
                StreamSupport.stream(ServiceLoader.load(TestService.class).spliterator(), false)
                        .collect(Collectors.toList());
        if (serviceList.size() != 1) {
            throw new RuntimeException("Require exactly one TestService implementation");
        }
        serviceList.get(0).connect();
    }
}
