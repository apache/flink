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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Either;

import java.lang.reflect.Type;
import java.util.Map;

public class EitherTypeInfoFactory<L, R> extends TypeInfoFactory<Either<L, R>> {

    @Override
    public TypeInformation<Either<L, R>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        TypeInformation<?> leftType = genericParameters.get("L");
        TypeInformation<?> rightType = genericParameters.get("R");

        if (leftType == null) {
            throw new InvalidTypesException(
                    "Type extraction is not possible on Either"
                            + " type as it does not contain information about the 'left' type.");
        }

        if (rightType == null) {
            throw new InvalidTypesException(
                    "Type extraction is not possible on Either"
                            + " type as it does not contain information about the 'right' type.");
        }

        return new EitherTypeInfo(leftType, rightType);
    }
}
