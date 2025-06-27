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

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture rules to prevent direct access to certain checkpointing configuration options. These
 * rules help ensure that helper methods with proper validation logic are used instead of direct
 * Configuration.get() or getOptional() calls.
 */
public class CheckpointingConfigurationAccessRules {

    private static final String CHECKPOINTING_OPTIONS_CLASS =
            "org.apache.flink.configuration.CheckpointingOptions";

    // Prohibited configuration fields that should not be used with Configuration.get() or
    // Configuration.getOptional()
    private static final Set<String> PROHIBITED_CONFIG_FIELDS =
            new HashSet<>(
                    Arrays.asList(
                            "ENABLE_UNALIGNED",
                            "CHECKPOINTING_CONSISTENCY_MODE",
                            "ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS"));

    // Whitelist: classes and their methods that are allowed to access these fields
    private static final Map<String, Set<String>> ALLOWED_CLASSES_AND_METHODS =
            new HashMap<>() {
                {
                    put(
                            CHECKPOINTING_OPTIONS_CLASS,
                            new HashSet<>(
                                    List.of(
                                            "isUnalignedCheckpointEnabled",
                                            "isCheckpointingEnabled",
                                            "getCheckpointingMode",
                                            "isUnalignedCheckpointInterruptibleTimersEnabled")));
                    put(
                            "org.apache.flink.streaming.api.environment.CheckpointConfig",
                            new HashSet<>(List.of("configure")));
                }
            };

    @ArchTest
    public static final ArchRule SHOULD_NOT_DIRECTLY_ACCESS_PROHIBITED_CONFIG_OPTIONS =
            noClasses()
                    .should()
                    .callMethodWhere(
                            new DescribedPredicate<>(
                                    "Configuration.get() or Configuration.getOptional() calls with prohibited CheckpointingOptions") {
                                @Override
                                public boolean test(JavaMethodCall call) {
                                    // Check if this is a call to Configuration.get() or
                                    // Configuration.getOptional()
                                    if (!(call.getTarget().getName().equals("get")
                                                    || call.getTarget()
                                                            .getName()
                                                            .equals("getOptional"))
                                            || !call.getTargetOwner()
                                                    .isAssignableTo(
                                                            org.apache.flink.configuration
                                                                    .Configuration.class)) {
                                        return false;
                                    }

                                    // Allow calls from within CheckpointingOptions class itself
                                    if (call.getOriginOwner()
                                            .getName()
                                            .equals(CHECKPOINTING_OPTIONS_CLASS)) {
                                        return false;
                                    }

                                    // Allow calls from whitelisted classes and methods
                                    String originClassName = call.getOriginOwner().getName();
                                    String originMethodName = call.getOrigin().getName();

                                    if (ALLOWED_CLASSES_AND_METHODS.containsKey(originClassName)) {
                                        Set<String> allowedMethods =
                                                ALLOWED_CLASSES_AND_METHODS.get(originClassName);
                                        if (allowedMethods.contains(originMethodName)) {
                                            return false;
                                        }
                                    }

                                    // Check if any prohibited config field is accessed in the same
                                    // method
                                    // This is a heuristic approach - we look for field access in
                                    // the same origin method
                                    boolean hasProhibitedFieldAccess =
                                            call.getOrigin().getFieldAccesses().stream()
                                                    .anyMatch(
                                                            CheckpointingConfigurationAccessRules
                                                                    ::isProhibitedConfigFieldAccess);

                                    return hasProhibitedFieldAccess;
                                }
                            })
                    .allowEmptyShould(true) // Allow until we refactor all existing usages
                    .because(
                            """
                                    Direct use of certain CheckpointingOptions configuration fields with Configuration.get() or Configuration.getOptional() should be avoided. \
                                    Use the appropriate helper methods which include proper validation logic:
                                    - ENABLE_UNALIGNED: Use CheckpointingOptions.isUnalignedCheckpointEnabled(Configuration)
                                    - CHECKPOINTING_CONSISTENCY_MODE: Use CheckpointingOptions.getCheckpointingMode(Configuration)
                                    - ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS: Use CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(Configuration)""");

    private static boolean isProhibitedConfigFieldAccess(JavaFieldAccess fieldAccess) {
        return PROHIBITED_CONFIG_FIELDS.contains(fieldAccess.getTarget().getName())
                && fieldAccess.getTargetOwner().getName().equals(CHECKPOINTING_OPTIONS_CLASS);
    }
}
