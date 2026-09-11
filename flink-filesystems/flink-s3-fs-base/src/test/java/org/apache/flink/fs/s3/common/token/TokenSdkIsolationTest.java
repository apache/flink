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

package org.apache.flink.fs.s3.common.token;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * Guards the AWS SDK isolation of the shared classes in {@code flink-s3-fs-base}.
 *
 * <p>The classes bundled into both S3 filesystem plugins must not reference AWS SDK types: the
 * {@code flink-s3-fs-presto} jar ships only AWS SDK v1 and the {@code flink-s3-fs-hadoop} jar only
 * AWS SDK v2, so a stray dependency turns into {@code NoClassDefFoundError} at runtime in one of
 * the plugins — invisible to unit tests, whose classpath contains both SDKs.
 */
class TokenSdkIsolationTest {

    private static final JavaClasses PRODUCTION_CLASSES =
            new ClassFileImporter()
                    .withImportOption(new ImportOption.DoNotIncludeTests())
                    .importPackages("org.apache.flink.fs.s3.common");

    @Test
    void tokenClassesMustStaySdkAgnostic() {
        classes()
                .that()
                .resideInAPackage("org.apache.flink.fs.s3.common.token")
                .and()
                .doNotHaveFullyQualifiedName(DynamicTemporaryAWSCredentialsProvider.NAME)
                .should()
                .onlyDependOnClassesThat()
                .resideOutsideOfPackages("software.amazon.awssdk..", "com.amazonaws..")
                .because(
                        "token state is shared between the SDK v1 presto plugin and the SDK v2 "
                                + "hadoop plugin")
                .check(PRODUCTION_CLASSES);
    }

    @Test
    void sdkV1CredentialsProviderMustNotDependOnSdkV2OrHadoopS3a() {
        classes()
                .that()
                .haveFullyQualifiedName(DynamicTemporaryAWSCredentialsProvider.NAME)
                .should()
                .onlyDependOnClassesThat()
                .resideOutsideOfPackages("software.amazon.awssdk..", "org.apache.hadoop.fs.s3a..")
                .because(
                        "the provider is loaded inside the presto plugin where SDK v2 is absent, "
                                + "and Hadoop 3.4's s3a exception hierarchy is based on SDK v2")
                .check(PRODUCTION_CLASSES);
    }

    @Test
    void flinkS3FileSystemMustStaySdkAgnostic() {
        classes()
                .that()
                .haveNameMatching(
                        "org\\.apache\\.flink\\.fs\\.s3\\.common\\.FlinkS3FileSystem(\\$.*)?")
                .should()
                .onlyDependOnClassesThat()
                .resideOutsideOfPackages("software.amazon.awssdk..", "com.amazonaws..")
                .because(
                        "the s5cmd credential lookup runs in both plugins and must not touch "
                                + "SDK-specific credential types")
                .check(PRODUCTION_CLASSES);
    }
}
