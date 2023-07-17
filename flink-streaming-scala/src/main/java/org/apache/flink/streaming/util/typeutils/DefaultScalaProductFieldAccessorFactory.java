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

package org.apache.flink.streaming.util.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;

import scala.Product;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Scala variant of {@link FieldAccessorFactory} for Scala products.
 *
 * <p>There are two versions of ProductFieldAccessor, differing in whether there is another
 * FieldAccessor nested inside. The no inner accessor version is probably a little faster.
 *
 * @deprecated All Flink Scala APIs are deprecated and will be removed in a future Flink major
 *     version. You can still build your application in Scala, but you should move to the Java
 *     version of either the DataStream and/or Table API.
 * @see <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@Deprecated
public class DefaultScalaProductFieldAccessorFactory implements ScalaProductFieldAccessorFactory {

    public <T, F> FieldAccessor<T, F> createSimpleProductFieldAccessor(
            int pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
        return new SimpleProductFieldAccessor<>(pos, typeInfo, config);
    }

    public <T, R, F> FieldAccessor<T, F> createRecursiveProductFieldAccessor(
            int pos,
            TypeInformation<T> typeInfo,
            FieldAccessor<R, F> innerAccessor,
            ExecutionConfig config) {
        return new RecursiveProductFieldAccessor<>(pos, typeInfo, innerAccessor, config);
    }

    private static final class SimpleProductFieldAccessor<T, F> extends FieldAccessor<T, F> {

        private static final long serialVersionUID = 1L;

        private final int pos;
        private final TupleSerializerBase<T> serializer;
        private final Object[] fields;
        private final int length;

        SimpleProductFieldAccessor(int pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
            checkNotNull(typeInfo, "typeInfo must not be null.");
            int arity = ((TupleTypeInfoBase) typeInfo).getArity();
            if (pos < 0 || pos >= arity) {
                throw new CompositeType.InvalidFieldReferenceException(
                        "Tried to select "
                                + ((Integer) pos).toString()
                                + ". field on \""
                                + typeInfo.toString()
                                + "\", which is an invalid index.");
            }

            this.pos = pos;
            this.fieldType = ((TupleTypeInfoBase<T>) typeInfo).getTypeAt(pos);
            this.serializer = (TupleSerializerBase<T>) typeInfo.createSerializer(config);
            this.length = this.serializer.getArity();
            this.fields = new Object[this.length];
        }

        @SuppressWarnings("unchecked")
        @Override
        public F get(T record) {
            Product prod = (Product) record;
            return (F) prod.productElement(pos);
        }

        @Override
        public T set(T record, F fieldValue) {
            Product prod = (Product) record;
            for (int i = 0; i < length; i++) {
                fields[i] = prod.productElement(i);
            }
            fields[pos] = fieldValue;
            return serializer.createInstance(fields);
        }
    }

    private static final class RecursiveProductFieldAccessor<T, R, F> extends FieldAccessor<T, F> {

        private static final long serialVersionUID = 1L;

        private final int pos;
        private final TupleSerializerBase<T> serializer;
        private final Object[] fields;
        private final int length;
        private final FieldAccessor<R, F> innerAccessor;

        RecursiveProductFieldAccessor(
                int pos,
                TypeInformation<T> typeInfo,
                FieldAccessor<R, F> innerAccessor,
                ExecutionConfig config) {
            int arity = ((TupleTypeInfoBase) typeInfo).getArity();
            if (pos < 0 || pos >= arity) {
                throw new CompositeType.InvalidFieldReferenceException(
                        "Tried to select "
                                + ((Integer) pos).toString()
                                + ". field on \""
                                + typeInfo.toString()
                                + "\", which is an invalid index.");
            }
            checkNotNull(typeInfo, "typeInfo must not be null.");
            checkNotNull(innerAccessor, "innerAccessor must not be null.");

            this.pos = pos;
            this.serializer = (TupleSerializerBase<T>) typeInfo.createSerializer(config);
            this.length = this.serializer.getArity();
            this.fields = new Object[this.length];
            this.innerAccessor = innerAccessor;
            this.fieldType = innerAccessor.getFieldType();
        }

        @SuppressWarnings("unchecked")
        @Override
        public F get(T record) {
            return innerAccessor.get((R) ((Product) record).productElement(pos));
        }

        @SuppressWarnings("unchecked")
        @Override
        public T set(T record, F fieldValue) {
            Product prod = (Product) record;
            for (int i = 0; i < length; i++) {
                fields[i] = prod.productElement(i);
            }
            fields[pos] = innerAccessor.set((R) fields[pos], fieldValue);
            return serializer.createInstance(fields);
        }
    }
}
