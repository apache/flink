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

package org.apache.flink.table.client.cli;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.guava18.com.google.common.reflect.ClassPath;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * CliStrings test class.
 */
public class CliStringsITCase {

    private final String space = " ";
    private final String empty = "";
    final String lineBreaker = "\n";

    /**
     * Test {@link org.apache.flink.table.client.cli.CliStrings#MESSAGE_HELP} contains all
     * commands' help descriptions in package {@link org.apache.flink.table.operations.command}.
     *
     * @throws IOException exception.
     */
    @Test
    public void testCompletenessOfCliClientCommandsInHelp() throws IOException {
        final String operationSuffix = "OPERATION";
        final String targetPackage = "org.apache.flink.table.operations.command";
        List<String> hintsInHelpMessage = getCommandsHintsInHelpMessage();
        List<String> hintsInPackage = getCommandsHintsInPackage(targetPackage, operationSuffix);
        if (CollectionUtils.isNotEmpty(hintsInPackage)) {
            if (CollectionUtils.isNotEmpty(hintsInHelpMessage)) {
                for (String hintInPkg : hintsInPackage) {
                    boolean containable = false;
                    for (String hintInHelpMsg : hintsInHelpMessage) {
                        if (hintInHelpMsg.contains(hintInPkg)) {
                            containable = true;
                            break;
                        }
                    }
                    Assert.assertTrue(containable);
                }
            } else {
                Assert.fail(
                        "CliStrings.HELP_MESSAGE doesn't contain commands completely in package 'org.apache.flink.table.operations.command'.");
            }
        }
    }

    /**
     * Get Commands Hints in package {@link org.apache.flink.table.operations.command}.
     *
     * @return A list which contains operation name in upper case without 'Operation' suffix
     *         and space.
     *
     * @throws IOException exception.
     */
    public List<String> getCommandsHintsInPackage(String packageName, String suffix)
            throws IOException {
        ImmutableSet<ClassPath.ClassInfo> topLevelClasses =
                ClassPath.from(Thread.currentThread().getContextClassLoader())
                        .getTopLevelClasses(packageName);
        return topLevelClasses.stream()
                .map(classInfo -> classInfo.getSimpleName().toUpperCase()).map(upperClassname -> {
                    int posStartSuffix = upperClassname.lastIndexOf(suffix);
                    return posStartSuffix > -1 ? upperClassname.substring(0, posStartSuffix) : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Get Commands Hints in {@link org.apache.flink.table.client.cli.CliStrings#MESSAGE_HELP}.
     *
     * @return A list which contains operation name in upper case.
     *
     * @throws IOException exception.
     */
    private List<String> getCommandsHintsInHelpMessage() {
        return Arrays
                .stream(CliStrings.MESSAGE_HELP.toString().split(lineBreaker))
                .filter(line -> StringUtils.isNotBlank(line)
                        && line.contains(CliStrings.CMD_DESC_DELIMITER))
                .map(line -> line.split(CliStrings.CMD_DESC_DELIMITER)[0]
                        .toUpperCase()
                        .replaceAll(space, empty))
                .collect(Collectors.toList());
    }
}







