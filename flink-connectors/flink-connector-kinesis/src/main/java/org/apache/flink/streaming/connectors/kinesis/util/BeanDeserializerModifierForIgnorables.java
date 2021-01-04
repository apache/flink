/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.util;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBuilder;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Jackson bean deserializer utility that allows skipping of properties, for example because they
 * cannot be handled by the default serializer or should be ignored for other reason.
 *
 * <p>Original source:
 * https://stackoverflow.com/questions/12305438/jackson-dynamic-filtering-of-properties-during-deserialization
 */
public class BeanDeserializerModifierForIgnorables extends BeanDeserializerModifier {

    private Class<?> type;
    private List<String> ignorables;

    public BeanDeserializerModifierForIgnorables(Class clazz, String... properties) {
        ignorables = new ArrayList<>();
        for (String property : properties) {
            ignorables.add(property);
        }
        this.type = clazz;
    }

    @Override
    public BeanDeserializerBuilder updateBuilder(
            DeserializationConfig config,
            BeanDescription beanDesc,
            BeanDeserializerBuilder builder) {
        if (!type.equals(beanDesc.getBeanClass())) {
            return builder;
        }

        for (String ignorable : ignorables) {
            builder.addIgnorable(ignorable);
        }
        return builder;
    }

    @Override
    public List<BeanPropertyDefinition> updateProperties(
            DeserializationConfig config,
            BeanDescription beanDesc,
            List<BeanPropertyDefinition> propDefs) {
        if (!type.equals(beanDesc.getBeanClass())) {
            return propDefs;
        }

        List<BeanPropertyDefinition> newPropDefs = new ArrayList<>();
        for (BeanPropertyDefinition propDef : propDefs) {
            if (!ignorables.contains(propDef.getName())) {
                newPropDefs.add(propDef);
            }
        }
        return newPropDefs;
    }
}
