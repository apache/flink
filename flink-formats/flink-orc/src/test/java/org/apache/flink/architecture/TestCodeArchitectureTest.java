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

import org.apache.flink.architecture.common.ImportOptions;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/** Architecture tests for test code. */
@AnalyzeClasses(
        packages = {"org.apache.flink.orc"},
        importOptions = {
            ImportOption.OnlyIncludeTests.class,
            ImportOptions.ExcludeScalaImportOption.class,
            ImportOptions.ExcludeShadedImportOption.class
        })
class TestCodeArchitectureTest {

    @ArchTest
    public static final ArchTests COMMON_TESTS = ArchTests.in(TestCodeArchitectureTestBase.class);
}
