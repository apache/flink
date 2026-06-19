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
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tngtech.archunit.lang.conditions.ArchPredicates.is;
import static org.apache.flink.architecture.common.JavaFieldPredicates.annotatedWith;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isAssignableTo;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isFinal;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isNotStatic;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isPublic;
import static org.apache.flink.architecture.common.JavaFieldPredicates.isStatic;
import static org.apache.flink.architecture.common.JavaFieldPredicates.ofType;

/**
 * Common predicates for architecture tests.
 *
 * <p>NOTE: it is recommended to use methods that accept fully qualified class names instead of
 * {@code Class} objects to reduce the risks of introducing circular dependencies between the
 * project submodules.
 */
public class Predicates {

    @SafeVarargs
    public static DescribedPredicate<JavaClass> areDirectlyAnnotatedWithAtLeastOneOf(
            Class<? extends Annotation>... annotations) {
        return Arrays.stream(annotations)
                .map(CanBeAnnotated.Predicates::annotatedWith)
                .reduce((p, pOther) -> p.or(pOther))
                .orElseThrow(IllegalArgumentException::new)
                .forSubtype();
    }

    /**
     * @return A {@link DescribedPredicate} returning true, if and only if the predicate {@link
     *     JavaField} could be found in the {@link JavaClass}.
     */
    public static DescribedPredicate<JavaClass> containAnyFieldsInClassHierarchyThat(
            DescribedPredicate<? super JavaField> predicate) {
        return new ContainAnyFieldsThatPredicate<>("fields", JavaClass::getAllFields, predicate);
    }

    /**
     * Tests that the given field is {@code public static} and has the fully qualified type name of
     * {@code fqClassName}.
     *
     * <p>Attention: changing the description will add a rule into the stored.rules.
     */
    public static DescribedPredicate<JavaField> arePublicStaticOfType(String fqClassName) {
        return areFieldOfType(fqClassName, JavaModifier.PUBLIC, JavaModifier.STATIC);
    }

    /**
     * Tests that the field has the fully qualified type of {@code fqClassName} with the given
     * modifiers.
     *
     * <p>Attention: changing the description will add a rule into the stored.rules.
     */
    public static DescribedPredicate<JavaField> areFieldOfType(
            String fqClassName, JavaModifier... modifiers) {
        return DescribedPredicate.describe(
                String.format(
                        "are %s, and of type %s",
                        Arrays.stream(modifiers)
                                .map(JavaModifier::toString)
                                .map(String::toLowerCase)
                                .collect(Collectors.joining(", ")),
                        getClassSimpleNameFromFqName(fqClassName)),
                field ->
                        field.getModifiers().containsAll(Arrays.asList(modifiers))
                                && field.getRawType().getName().equals(fqClassName));
    }

    /**
     * Tests that the given field is {@code public final}, not {@code static} and has the given
     * fully qualified type name of {@code fqClassName}.
     */
    public static DescribedPredicate<JavaField> arePublicFinalOfType(String fqClassName) {
        return is(ofType(fqClassName)).and(isPublic()).and(isFinal()).and(isNotStatic());
    }

    /**
     * Tests that the given field is {@code public static final} and is assignable to the given type
     * {@code clazz} .
     */
    public static DescribedPredicate<JavaField> arePublicStaticFinalAssignableTo(Class<?> clazz) {
        return is(isAssignableTo(clazz)).and(isPublic()).and(isStatic()).and(isFinal());
    }

    /**
     * Tests that the field is {@code public static final} and has the fully qualified type name of
     * {@code fqClassName}.
     */
    public static DescribedPredicate<JavaField> arePublicStaticFinalOfType(String fqClassName) {
        return arePublicStaticOfType(fqClassName).and(isFinal());
    }

    /**
     * Tests that the field is {@code public final}, has the fully qualified type name of {@code
     * fqClassName} and is annotated with the {@code annotationType}.
     */
    public static DescribedPredicate<JavaField> arePublicFinalOfTypeWithAnnotation(
            String fqClassName, Class<? extends Annotation> annotationType) {
        return arePublicFinalOfType(fqClassName).and(annotatedWith(annotationType));
    }

    /**
     * Tests that the field is {@code public static final}, has the fully qualified type name of
     * {@code fqClassName} and is annotated with the {@code annotationType}.
     */
    public static DescribedPredicate<JavaField> arePublicStaticFinalOfTypeWithAnnotation(
            String fqClassName, Class<? extends Annotation> annotationType) {
        return arePublicStaticFinalOfType(fqClassName).and(annotatedWith(annotationType));
    }

    /**
     * Tests that the field is {@code static final}, has the fully qualified type name of {@code
     * fqClassName} and is annotated with the {@code annotationType}. It doesn't matter if public,
     * private or protected.
     */
    public static DescribedPredicate<JavaField> areStaticFinalOfTypeWithAnnotation(
            String fqClassName, Class<? extends Annotation> annotationType) {
        return areFieldOfType(fqClassName, JavaModifier.STATIC, JavaModifier.FINAL)
                .and(annotatedWith(annotationType));
    }

    /**
     * Returns a {@link DescribedPredicate} that returns true if one and only one of the given
     * predicates match.
     */
    @SafeVarargs
    public static <T> DescribedPredicate<T> exactlyOneOf(
            final DescribedPredicate<? super T>... other) {
        return DescribedPredicate.describe(
                "only one of the following predicates match:\n"
                        + Arrays.stream(other)
                                .map(dp -> "* " + dp + "\n")
                                .collect(Collectors.joining()),
                t -> Arrays.stream(other).map(dp -> dp.test(t)).reduce(false, Boolean::logicalXor));
    }

    /**
     * Extracts the class name from the given fully qualified class name.
     *
     * <p>Example:
     *
     * <pre>
     *     getClassFromFqName("com.example.MyClass");  // Returns: "MyClass"
     * </pre>
     */
    public static String getClassSimpleNameFromFqName(String fqClassName) {
        // Not using Preconditions to avoid adding non-test flink-core dependency
        if (fqClassName == null) {
            throw new NullPointerException("Fully qualified class name cannot be null");
        }
        if (fqClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("Fully qualified class name cannot be empty");
        }
        int lastDotIndex = fqClassName.lastIndexOf('.');
        int lastDollarIndex = fqClassName.lastIndexOf('$');
        int startIndex = Math.max(lastDotIndex, lastDollarIndex) + 1;
        String className = fqClassName.substring(startIndex);
        if (className.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Extracted class name is empty from: " + fqClassName);
        }
        return className;
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
        public boolean test(JavaClass input) {
            for (T member : getFields.apply(input)) {
                if (predicate.test(member)) {
                    return true;
                }
            }
            return false;
        }
    }
}
