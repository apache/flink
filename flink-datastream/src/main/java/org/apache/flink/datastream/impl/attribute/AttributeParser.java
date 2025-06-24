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

package org.apache.flink.datastream.impl.attribute;

import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.datastream.api.attribute.NoOutputUntilEndOfInput;
import org.apache.flink.datastream.api.function.ProcessFunction;

/** {@link AttributeParser} is used to parse {@link Attribute} from {@link ProcessFunction}. */
public class AttributeParser {

    public static Attribute parseAttribute(ProcessFunction function) {
        Class<? extends ProcessFunction> functionClass = function.getClass();
        Attribute.Builder attributeBuilder = new Attribute.Builder();
        if (functionClass.isAnnotationPresent(NoOutputUntilEndOfInput.class)) {
            attributeBuilder.setNoOutputUntilEndOfInput(true);
        }
        return attributeBuilder.build();
    }
}
