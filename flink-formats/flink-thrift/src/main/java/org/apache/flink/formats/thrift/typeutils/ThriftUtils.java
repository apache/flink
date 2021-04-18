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

package org.apache.flink.formats.thrift.typeutils;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.protocol.TTupleProtocol;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Map;

/** Thrift related utility functions. */
public class ThriftUtils {

    @Nullable
    public static TFieldIdEnum getFieldIdEnum(
            @Nonnull Class<? extends TBase> thriftClass, String fieldName) {
        try {
            Class<?>[] classes = thriftClass.getDeclaredClasses();
            Class<?> fieldsClazz = null;

            for (Class<?> clazz : classes) {
                if (clazz.getSimpleName().equals("_Fields")) {
                    fieldsClazz = clazz;
                    break;
                }
            }
            Method findByNameMethod = fieldsClazz.getMethod("findByName", String.class);
            TFieldIdEnum result = (TFieldIdEnum) findByNameMethod.invoke(fieldsClazz, fieldName);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public static FieldValueMetaData getFieldMetaData(
            @Nonnull Class<? extends TBase> thriftClass, String fieldName) {
        final Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap =
                FieldMetaData.getStructMetaDataMap(thriftClass);
        TFieldIdEnum tFieldIdEnum = getFieldIdEnum(thriftClass, fieldName);
        if (tFieldIdEnum != null) {
            return metaDataMap.get(tFieldIdEnum).valueMetaData;
        }
        return null;
    }

    @Nullable
    public static TProtocolFactory getTProtocolFactory(String protocolName) {
        switch (protocolName) {
            case "org.apache.thrift.protocol.TCompactProtocol":
                return new TCompactProtocol.Factory();
            case "org.apache.thrift.protocol.TBinaryProtocol":
                return new TBinaryProtocol.Factory();
            case "org.apache.thrift.protocol.TJSONProtocol":
                return new TJSONProtocol.Factory();
            case "org.apache.thrift.protocol.TSimpleJSONProtocol":
                return new TSimpleJSONProtocol.Factory();
            case "org.apache.thrift.protocol.TTupleProtocol":
                return new TTupleProtocol.Factory();
            default:
                return null;
        }
    }
}
