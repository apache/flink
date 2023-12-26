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

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

/** Rules for modules already completed the migration of junit5. */
public class BanJunit4Rules {

    @ArchTest
    public static final ArchRule NO_NEW_ADDED_JUNIT4_TEST_RULE =
            freeze(
                    noClasses()
                            .that()
                            .resideInAPackage("org.apache.flink..")
                            .should()
                            .dependOnClassesThat()
                            .resideInAnyPackage("junit", "org.junit")
                            .as("Junit4 is forbidden, please use Junit5 instead"));
}
