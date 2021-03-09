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

package org.apache.flink.test.completeness;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/** Scans the class path for type information and checks if there is a test for it. */
public class TypeInfoTestCoverageTest extends TestLogger {

    @Test
    public void testTypeInfoTestCoverage() {
        Reflections reflections = new Reflections("org.apache.flink");

        Set<Class<? extends TypeInformation>> typeInfos =
                reflections.getSubTypesOf(TypeInformation.class);

        Set<String> typeInfoTestNames =
                reflections.getSubTypesOf(TypeInformationTestBase.class).stream()
                        .map(Class::getName)
                        .collect(Collectors.toSet());

        // check if a test exists for each type information
        for (Class<? extends TypeInformation> typeInfo : typeInfos) {
            // we skip abstract classes and inner classes to skip type information defined in test
            // classes
            if (Modifier.isAbstract(typeInfo.getModifiers())
                    || Modifier.isPrivate(typeInfo.getModifiers())
                    || typeInfo.getName().contains("Test$")
                    || typeInfo.getName().contains("TestBase$")
                    || typeInfo.getName().contains("ITCase$")
                    || typeInfo.getName().contains("$$anon")
                    || typeInfo.getName().contains("queryablestate")) {
                continue;
            }

            final String testToFind = typeInfo.getName() + "Test";
            if (!typeInfoTestNames.contains(testToFind)) {
                fail(
                        "Could not find test '"
                                + testToFind
                                + "' that covers '"
                                + typeInfo.getName()
                                + "'.");
            }
        }
    }
}
