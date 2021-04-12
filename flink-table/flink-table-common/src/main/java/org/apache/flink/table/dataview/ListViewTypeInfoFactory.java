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

package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.dataview.ListView;

import java.lang.reflect.Type;
import java.util.Map;

/** TypeInformation factory for {@link ListView}. */
@Internal
@Deprecated
public class ListViewTypeInfoFactory<T> extends TypeInfoFactory<ListView<T>> {

    @Override
    public TypeInformation<ListView<T>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        TypeInformation<?> elementType = genericParameters.get("T");

        if (elementType == null) {
            // we might can get the elementType later from the ListView constructor
            elementType = new GenericTypeInfo<>(Object.class);
        }

        //noinspection unchecked
        return new ListViewTypeInfo<>((TypeInformation<T>) elementType);
    }
}
