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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Validator for {@link ClassInstance}. */
@Internal
public class ClassInstanceValidator extends HierarchyDescriptorValidator {

    public static final String CLASS = "class";
    public static final String CONSTRUCTOR = "constructor";

    /** @param keyPrefix prefix to be added to every property before validation */
    public ClassInstanceValidator(String keyPrefix) {
        super(keyPrefix);
    }

    public ClassInstanceValidator() {
        this(EMPTY_PREFIX);
    }

    @Override
    protected void validateWithPrefix(String keyPrefix, DescriptorProperties properties) {
        // check class name
        properties.validateString(keyPrefix + CLASS, false, 1);

        // check constructor
        String constructorPrefix = keyPrefix + CONSTRUCTOR;

        List<Map<String, String>> constructorProperties =
                properties.getVariableIndexedProperties(constructorPrefix, new ArrayList<>());
        for (int i = 0; i < constructorProperties.size(); ++i) {
            String keyPrefixWithIdx = constructorPrefix + "." + i + ".";
            if (constructorProperties.get(i).containsKey(ClassInstanceValidator.CLASS)) {
                ClassInstanceValidator classInstanceValidator =
                        new ClassInstanceValidator(keyPrefixWithIdx);
                classInstanceValidator.validate(properties);
            }
            // literal value
            else {
                LiteralValueValidator primitiveValueValidator =
                        new LiteralValueValidator(keyPrefixWithIdx);
                primitiveValueValidator.validate(properties);
            }
        }
    }
}
