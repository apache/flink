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
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.lang.syntax.elements.ClassesThat;
import com.tngtech.archunit.lang.syntax.elements.GivenClassesConjunction;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.apache.flink.architecture.common.SourcePredicates.areJavaClasses;

/**
 * Equivalent of {@link ArchRuleDefinition#classes()} and similar methods with a restriction on Java
 * classes.
 *
 * <p>ArchUnit does not yet fully support Scala. Rules should therefore use these methods instead to
 * restrict themselves to Java classes.
 */
public class GivenJavaClasses {

    /** Equivalent of {@link ArchRuleDefinition#classes()}, but only for Java classes. */
    public static ClassesThat<GivenClassesConjunction> javaClassesThat() {
        return classes().that(areJavaClasses()).and();
    }

    /** Equivalent of {@link ArchRuleDefinition#classes()}, but only for Java classes. */
    public static GivenClassesConjunction javaClassesThat(DescribedPredicate<JavaClass> predicate) {
        return classes().that(areJavaClasses()).and(predicate);
    }

    /** Equivalent of {@link ArchRuleDefinition#noClasses()}, but only for Java classes. */
    public static ClassesThat<GivenClassesConjunction> noJavaClassesThat() {
        return noClasses().that(areJavaClasses()).and();
    }

    /** Equivalent of {@link ArchRuleDefinition#noClasses()}, but only for Java classes. */
    public static GivenClassesConjunction noJavaClassesThat(
            DescribedPredicate<JavaClass> predicate) {
        return noClasses().that(areJavaClasses()).and(predicate);
    }

    private GivenJavaClasses() {}
}
