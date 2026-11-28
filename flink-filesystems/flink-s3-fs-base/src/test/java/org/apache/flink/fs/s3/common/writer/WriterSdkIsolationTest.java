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

package org.apache.flink.fs.s3.common.writer;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * The writer classes are bundled into the presto plugin too, which ships no AWS SDK v2. Unit tests
 * have both SDKs on the classpath, so only this rule catches an SDK type in the writer API.
 */
class WriterSdkIsolationTest {

    @Test
    void writerClassesMustStaySdkAgnostic() {
        final JavaClasses writerClasses =
                new ClassFileImporter()
                        .withImportOption(new ImportOption.DoNotIncludeTests())
                        .importPackages("org.apache.flink.fs.s3.common.writer");

        classes()
                .should()
                .onlyDependOnClassesThat()
                .resideOutsideOfPackages("software.amazon.awssdk..", "com.amazonaws..")
                .because("the presto plugin bundles these classes without AWS SDK v2")
                .check(writerClasses);
    }
}
