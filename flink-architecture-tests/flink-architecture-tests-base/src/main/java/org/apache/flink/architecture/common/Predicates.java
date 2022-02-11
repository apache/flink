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

import com.tngtech.archunit.base.ChainableFunction;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.base.Function;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;

import static com.tngtech.archunit.lang.conditions.ArchPredicates.is;
import static org.apache.flink.architecture.common.JavaFieldPredicates.annotatedWith;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isFinal;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isNotStatic;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isPublic;
import static org.apache.flink.architecture.common.JavaFieldPredicates.ofType;

/** Common predicates for architecture tests. */
public class Predicates {

    @SafeVarargs
    public static DescribedPredicate<JavaClass> areDirectlyAnnotatedWithAtLeastOneOf(
            Class<? extends Annotation>... annotations) {
        return Arrays.stream(annotations)
                .map(CanBeAnnotated.Predicates::annotatedWith)
                .reduce(DescribedPredicate::or)
                .orElseThrow(IllegalArgumentException::new)
                .forSubtype();
    }

    /**
     * @return A {@link DescribedPredicate} returning true, if and only if the predicate {@link
     *     JavaField} could be found in the {@link JavaClass}.
     */
    public static DescribedPredicate<JavaClass> containAnyFieldsInClassHierarchyThat(
            DescribedPredicate<? super JavaField> predicate) {
        return new ContainAnyFieldsThatPredicate<>(
                "fields",
                new ChainableFunction<JavaClass, Set<JavaField>>() {
                    @Override
                    public Set<JavaField> apply(JavaClass input) {
                        // need to get all fields with the inheritance hierarchy
                        return input.getAllFields();
                    }
                },
                predicate);
    }

    /**
     * Tests that the given field is {@code public static} and of the given type {@code clazz} .
     *
     * <p>Attention: changing the description will add a rule into the stored.rules.
     */
    public static DescribedPredicate<JavaField> arePublicStaticOfType(Class<?> clazz) {
        return DescribedPredicate.describe(
                "are public, static, and of type " + clazz.getSimpleName(),
                field ->
                        field.getModifiers().contains(JavaModifier.PUBLIC)
                                && field.getModifiers().contains(JavaModifier.STATIC)
                                && field.getRawType().isEquivalentTo(clazz));
    }

    /**
     * Tests that the given field is {@code public final} and not {@code static} and of the given
     * type {@code clazz} .
     */
    public static DescribedPredicate<JavaField> arePublicFinalOfType(Class<?> clazz) {
        return is(ofType(clazz)).and(isPublic()).and(isFinal()).and(isNotStatic());
    }

    /**
     * Tests that the given field is {@code public static final} and of the given type {@code clazz}
     * .
     */
    public static DescribedPredicate<JavaField> arePublicStaticFinalOfType(Class<?> clazz) {
        return arePublicStaticOfType(clazz).and(isFinal());
    }

    /**
     * Tests that the given field is {@code public final} and of the given type {@code clazz} with
     * exactly the given {@code annotationType}.
     */
    public static DescribedPredicate<JavaField> arePublicFinalOfTypeWithAnnotation(
            Class<?> clazz, Class<? extends Annotation> annotationType) {
        return arePublicFinalOfType(clazz).and(annotatedWith(annotationType));
    }

    /**
     * Tests that the given field is {@code public static final} and of the given type {@code clazz}
     * with exactly the given {@code annotationType}.
     */
    public static DescribedPredicate<JavaField> arePublicStaticFinalOfTypeWithAnnotation(
            Class<?> clazz, Class<? extends Annotation> annotationType) {
        return arePublicStaticFinalOfType(clazz).and(annotatedWith(annotationType));
    }

    private Predicates() {}

    /**
     * A predicate to determine if a {@link JavaClass} contains one or more {@link JavaField }
     * matching the supplied predicate.
     */
    private static class ContainAnyFieldsThatPredicate<T extends JavaField>
            extends DescribedPredicate<JavaClass> {
        private final Function<JavaClass, Set<T>> getFields;
        private final DescribedPredicate<? super T> predicate;

        ContainAnyFieldsThatPredicate(
                String fieldDescription,
                Function<JavaClass, Set<T>> getFields,
                DescribedPredicate<? super T> predicate) {
            super("contain any " + fieldDescription + " that " + predicate.getDescription());
            this.getFields = getFields;
            this.predicate = predicate;
        }

        @Override
        public boolean apply(JavaClass input) {
            for (T member : getFields.apply(input)) {
                if (predicate.apply(member)) {
                    return true;
                }
            }
            return false;
        }
    }
}
