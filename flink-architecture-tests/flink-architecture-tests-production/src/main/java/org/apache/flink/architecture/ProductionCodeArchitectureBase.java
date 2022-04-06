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

package org.apache.flink.architecture;

import org.apache.flink.architecture.rules.ApiAnnotationRules;
import org.apache.flink.architecture.rules.TableApiRules;

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/**
 * Central setup of architectural tests for the production code.
 *
 * <p>Architectural tests should include this class via {@link ArchTests#in(Class)} to cover the
 * common part.
 */
public class ProductionCodeArchitectureBase {
    @ArchTest
    public static final ArchTests API_ANNOTATIONS = ArchTests.in(ApiAnnotationRules.class);

    @ArchTest public static final ArchTests TABLE_API = ArchTests.in(TableApiRules.class);
}
