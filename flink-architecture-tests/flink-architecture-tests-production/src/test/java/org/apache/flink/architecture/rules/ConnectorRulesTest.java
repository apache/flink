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

package org.apache.flink.architecture.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.core.fs.Path;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests connector dependency classification, including array component types. */
class ConnectorRulesTest {

    private static final JavaClass DEPENDENCIES =
            new ClassFileImporter().importClass(Dependencies.class);

    @ParameterizedTest
    @CsvSource({
        "publicScalar, true",
        "publicArray, true",
        "publicMatrix, true",
        "evolvingArray, true",
        "evolvingMatrix, true",
        "nestedPublicArray, true",
        "pathArray, true",
        "internalScalar, false",
        "internalArray, false",
        "internalMatrix, false",
        "unannotatedArray, false",
        "internalFilterArray, false",
        "externalArray, true",
        "primitiveArray, true"
    })
    void testAllowedDependency(String field, boolean allowed) {
        assertThat(
                        ConnectorRules.areAllowedDependencies()
                                .test(DEPENDENCIES.getField(field).getRawType()))
                .isEqualTo(allowed);
    }

    private static class Dependencies {
        private PublicType publicScalar;
        private PublicType[] publicArray;
        private PublicType[][] publicMatrix;
        private EvolvingType[] evolvingArray;
        private EvolvingType[][] evolvingMatrix;
        private PublicType.Nested[] nestedPublicArray;
        private Path[] pathArray;
        private InternalType internalScalar;
        private InternalType[] internalArray;
        private InternalType[][] internalMatrix;
        private UnannotatedType[] unannotatedArray;
        private GlobFilePathFilter[] internalFilterArray;
        private String[] externalArray;
        private int[] primitiveArray;
    }

    @Public
    private static class PublicType {
        private static class Nested {}
    }

    @PublicEvolving
    private static class EvolvingType {}

    @Internal
    private static class InternalType {}

    private static class UnannotatedType {}
}
