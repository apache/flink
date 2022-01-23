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

import org.apache.flink.architecture.common.GivenJavaClasses;
import org.apache.flink.architecture.common.SourcePredicates;
import org.apache.flink.architecture.rules.ApiAnnotationRules;
import org.apache.flink.architecture.rules.TableApiRules;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

import java.util.regex.Pattern;

/** Architecture tests. */
@AnalyzeClasses(
        packages = "org.apache.flink",
        importOptions = {
            ImportOption.DoNotIncludeTests.class,
            ArchitectureTest.ExcludeScalaImportOption.class,
            ArchitectureTest.ExcludeShadedImportOption.class
        })
public class ArchitectureTest {
    @ArchTest
    public static final ArchTests API_ANNOTATIONS = ArchTests.in(ApiAnnotationRules.class);

    @ArchTest public static final ArchTests TABLE_API = ArchTests.in(TableApiRules.class);

    // ---------------------------------------------------------------------------------------------

    /**
     * Excludes Scala classes on a best-effort basis.
     *
     * <p>ArchUnit doesn't yet support fully Scala. This is a best-effort attempt to not import
     * Scala classes in the first place. However, it is not perfect, and thus {@link
     * GivenJavaClasses} or {@link SourcePredicates#areJavaClasses()} should be used in rules as
     * well.
     */
    static class ExcludeScalaImportOption implements ImportOption {
        private static final Pattern SCALA = Pattern.compile(".*/scala/.*");

        @Override
        public boolean includes(Location location) {
            return !location.matches(SCALA);
        }
    }

    /**
     * Exclude locations that look shaded.
     *
     * <p>This is not only important to exclude external code shaded into a package like {@code
     * org.apache.flink.shaded.*} from being tested, but crucial for memory consumption.
     */
    static class ExcludeShadedImportOption implements ImportOption {
        private static final Pattern SHADED = Pattern.compile(".*/shaded/.*");

        @Override
        public boolean includes(Location location) {
            return !location.matches(SHADED);
        }
    }
}
