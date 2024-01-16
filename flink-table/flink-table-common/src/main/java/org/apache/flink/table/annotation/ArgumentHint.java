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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A hint that provides additional information about an argument.
 *
 * <p>An {@code ArgumentHint} can be used to provide hints about the name, optionality, and data
 * type of argument.
 *
 * <p>It combines the functionality of {@link FunctionHint#argumentNames()} and {@link DataTypeHint}
 * annotations to conveniently group argument-related information together in function declarations.
 *
 * <p>{@code @ArgumentHint(name = "in1", type = @DataTypeHint("STRING"), isOptional = false} is an
 * argument with the type String, named in1, and cannot be omitted when calling.
 */
@PublicEvolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
public @interface ArgumentHint {

    /**
     * The name of the argument.
     *
     * <p>This can be used to provide a descriptive name for the argument.
     */
    String name() default "";

    /**
     * Specifies whether the argument is optional or required.
     *
     * <p>If set to {@code true}, the argument is considered optional.And if the user does not
     * specify this parameter when calling, 'null' will be passed in. By default, an argument is
     * considered required.
     */
    boolean isOptional() default false;

    /**
     * The data type hint for the argument.
     *
     * <p>This can be used to provide additional information about the expected data type of the
     * argument. The {@link DataTypeHint} annotation can be used to specify the data type explicitly
     * or provide hints for the reflection-based extraction of the data type.
     */
    DataTypeHint type() default @DataTypeHint();
}
