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

package org.apache.flink.architecture.common;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.Source;

/** Predicates for a {@link JavaClass}'s {@link Source}. */
public class SourcePredicates {

    /**
     * Tests that a given class is a Java class.
     *
     * <p>ArchUnit does not yet fully support Scala. Rules should ensure that they restrict
     * themselves to only Java classes for correct results.
     */
    public static DescribedPredicate<JavaClass> areJavaClasses() {
        return new DescribedPredicate<JavaClass>("are Java classes") {
            @Override
            public boolean test(JavaClass clazz) {
                return isJavaClass(clazz);
            }
        };
    }

    /**
     * Checks whether the given {@link JavaClass} is actually a Java class, and not a Scala class.
     *
     * <p>ArchUnit does not yet fully support Scala. Rules should ensure that they restrict
     * themselves to only Java classes for correct results.
     */
    static boolean isJavaClass(JavaClass clazz) {
        if (!clazz.getSource().isPresent()) {
            return false;
        }

        final Source source = clazz.getSource().get();
        if (!source.getFileName().isPresent()) {
            return false;
        }

        return source.getFileName().get().contains(".java");
    }

    private SourcePredicates() {}
}
