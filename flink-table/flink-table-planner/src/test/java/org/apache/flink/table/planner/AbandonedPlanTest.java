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

package org.apache.flink.table.planner;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Test to check whether there is a plan without any corresponding test. */
class AbandonedPlanTest {
    @Test
    void testIfThereAreAbandonedPlans() {
        final Path resources = Paths.get("src", "test", "resources");
        final String packageName =
                AbandonedPlanTest.class.getPackageName().replace(".", File.separator);
        final Path resourcePath = Paths.get(resources.toString(), packageName);
        final Set<String> plans = new HashSet<>();
        walk(
                resourcePath,
                path -> {
                    if (path.toString().endsWith("Test.xml")) {
                        plans.add(
                                path.toString()
                                        .replace(resources.toString(), "")
                                        .replace(".xml", ""));
                    }
                    return FileVisitResult.CONTINUE;
                });

        final Path javaTestPath = Paths.get("src", "test", "java");
        final Path scalaTestPath = Paths.get("src", "test", "scala");
        final Path pathForJavaClasses = Paths.get(javaTestPath.toString(), packageName);
        walk(
                pathForJavaClasses,
                path -> {
                    if (path.toString().endsWith("Test.java")) {
                        plans.remove(
                                path.toString()
                                        .replace(javaTestPath.toString(), "")
                                        .replace(".java", ""));
                        if (plans.isEmpty()) {
                            return FileVisitResult.TERMINATE;
                        }
                    }
                    return FileVisitResult.CONTINUE;
                });

        final Path pathForScalaClasses = Paths.get(scalaTestPath.toString(), packageName);
        walk(
                pathForScalaClasses,
                path -> {
                    if (path.toString().endsWith("Test.scala")) {
                        plans.remove(
                                path.toString()
                                        .replace(scalaTestPath.toString(), "")
                                        .replace(".scala", ""));
                        if (plans.isEmpty()) {
                            return FileVisitResult.TERMINATE;
                        }
                    }
                    return FileVisitResult.CONTINUE;
                });

        assertThat(plans.stream().map(p -> p + ".xml"))
                .as("There are xml plans without corresponding java or scala tests")
                .isEmpty();
    }

    private void walk(Path startPath, Function<Path, FileVisitResult> function) {
        try {
            Files.walkFileTree(
                    startPath,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            return function.apply(file);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
