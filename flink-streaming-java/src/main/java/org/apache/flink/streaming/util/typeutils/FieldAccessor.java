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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.FieldSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;

import scala.Product;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * These classes encapsulate the logic of accessing a field specified by the user as either an index
 * or a field expression string. TypeInformation can also be requested for the field. The position
 * index might specify a field of a Tuple, an array, or a simple type(only "0th field").
 *
 * <p>Field expressions that specify nested fields (e.g. "f1.a.foo") result in nested field
 * accessors. These penetrate one layer, and then delegate the rest of the work to an
 * "innerAccessor". (see PojoFieldAccessor, RecursiveTupleFieldAccessor,
 * RecursiveProductFieldAccessor)
 */
@Internal
public abstract class FieldAccessor<T, F> implements Serializable {

    private static final long serialVersionUID = 1L;

    protected TypeInformation fieldType;

    /**
     * Gets the TypeInformation for the type of the field. Note: For an array of a primitive type,
     * it returns the corresponding basic type (Integer for int[]).
     */
    @SuppressWarnings("unchecked")
    public TypeInformation<F> getFieldType() {
        return fieldType;
    }

    /**
     * Gets the value of the field (specified in the constructor) of the given record.
     *
     * @param record The record on which the field will be accessed
     * @return The value of the field.
     */
    public abstract F get(T record);

    /**
     * Sets the field (specified in the constructor) of the given record to the given value.
     *
     * <p>Warning: This might modify the original object, or might return a new object instance.
     * (This is necessary, because the record might be immutable.)
     *
     * @param record The record to modify
     * @param fieldValue The new value of the field
     * @return A record that has the given field value. (this might be a new instance or the
     *     original)
     */
    public abstract T set(T record, F fieldValue);

    // --------------------------------------------------------------------------------------------------

    /**
     * This is when the entire record is considered as a single field. (eg. field 0 of a basic type,
     * or a field of a POJO that is itself some composite type but is not further decomposed)
     */
    static final class SimpleFieldAccessor<T> extends FieldAccessor<T, T> {

        private static final long serialVersionUID = 1L;

        public SimpleFieldAccessor(TypeInformation<T> typeInfo) {
            checkNotNull(typeInfo, "typeInfo must not be null.");

            this.fieldType = typeInfo;
        }

        @Override
        public T get(T record) {
            return record;
        }

        @Override
        public T set(T record, T fieldValue) {
            return fieldValue;
        }
    }

    static final class ArrayFieldAccessor<T, F> extends FieldAccessor<T, F> {

        private static final long serialVersionUID = 1L;

        private final int pos;

        public ArrayFieldAccessor(int pos, TypeInformation typeInfo) {
            if (pos < 0) {
                throw new CompositeType.InvalidFieldReferenceException(
                        "The "
                                + ((Integer) pos).toString()
                                + ". field selected on"
                                + " an array, which is an invalid index.");
            }
            checkNotNull(typeInfo, "typeInfo must not be null.");

            this.pos = pos;
            this.fieldType = BasicTypeInfo.getInfoFor(typeInfo.getTypeClass().getComponentType());
        }

        @SuppressWarnings("unchecked")
        @Override
        public F get(T record) {
            return (F) Array.get(record, pos);
        }

        @Override
        public T set(T record, F fieldValue) {
            Array.set(record, pos, fieldValue);
            return record;
        }
    }

    /**
     * There are two versions of TupleFieldAccessor, differing in whether there is an other
     * FieldAccessor nested inside. The no inner accessor version is probably a little faster.
     */
    static final class SimpleTupleFieldAccessor<T extends Tuple, F> extends FieldAccessor<T, F> {

        private static final long serialVersionUID = 1L;

        private final int pos;

        SimpleTupleFieldAccessor(int pos, TypeInformation<T> typeInfo) {
            checkNotNull(typeInfo, "typeInfo must not be null.");
            int arity = ((TupleTypeInfo) typeInfo).getArity();
            if (pos < 0 || pos >= arity) {
                throw new CompositeType.InvalidFieldReferenceException(
                        "Tried to select "
                                + ((Integer) pos).toString()
                                + ". field on \""
                                + typeInfo.toString()
                                + "\", which is an invalid index.");
            }

            this.pos = pos;
            this.fieldType = ((TupleTypeInfo) typeInfo).getTypeAt(pos);
        }

        @SuppressWarnings("unchecked")
        @Override
        public F get(T record) {
            return (F) record.getField(pos);
        }

        @Override
        public T set(T record, F fieldValue) {
            record.setField(fieldValue, pos);
            return record;
        }
    }

    /**
     * @param <T> The Tuple type
     * @param <R> The field type at the first level
     * @param <F> The field type at the innermost level
     */
    static final class RecursiveTupleFieldAccessor<T extends Tuple, R, F>
            extends FieldAccessor<T, F> {

        private static final long serialVersionUID = 1L;

        private final int pos;
        private final FieldAccessor<R, F> innerAccessor;

        RecursiveTupleFieldAccessor(
                int pos, FieldAccessor<R, F> innerAccessor, TypeInformation<T> typeInfo) {
            checkNotNull(typeInfo, "typeInfo must not be null.");
            checkNotNull(innerAccessor, "innerAccessor must not be null.");

            int arity = ((TupleTypeInfo) typeInfo).getArity();
            if (pos < 0 || pos >= arity) {
                throw new CompositeType.InvalidFieldReferenceException(
                        "Tried to select "
                                + ((Integer) pos).toString()
                                + ". field on \""
                                + typeInfo.toString()
                                + "\", which is an invalid index.");
            }

            this.pos = pos;
            this.innerAccessor = innerAccessor;
            this.fieldType = innerAccessor.fieldType;
        }

        @Override
        public F get(T record) {
            final R inner = record.getField(pos);
            return innerAccessor.get(inner);
        }

        @Override
        public T set(T record, F fieldValue) {
            final R inner = record.getField(pos);
            record.setField(innerAccessor.set(inner, fieldValue), pos);
            return record;
        }
    }

    /**
     * @param <T> The POJO type
     * @param <R> The field type at the first level
     * @param <F> The field type at the innermost level
     */
    static final class PojoFieldAccessor<T, R, F> extends FieldAccessor<T, F> {

        private static final long serialVersionUID = 1L;

        private transient Field field;
        private final FieldAccessor<R, F> innerAccessor;

        PojoFieldAccessor(Field field, FieldAccessor<R, F> innerAccessor) {
            checkNotNull(field, "field must not be null.");
            checkNotNull(innerAccessor, "innerAccessor must not be null.");

            this.field = field;
            this.innerAccessor = innerAccessor;
            this.fieldType = innerAccessor.fieldType;
        }

        @Override
        public F get(T pojo) {
            try {
                @SuppressWarnings("unchecked")
                final R inner = (R) field.get(pojo);
                return innerAccessor.get(inner);
            } catch (IllegalAccessException iaex) {
                // The Field class is transient and when deserializing its value we also make it
                // accessible
                throw new RuntimeException(
                        "This should not happen since we call setAccesssible(true) in readObject."
                                + " fields: "
                                + field
                                + " obj: "
                                + pojo);
            }
        }

        @Override
        public T set(T pojo, F valueToSet) {
            try {
                @SuppressWarnings("unchecked")
                final R inner = (R) field.get(pojo);
                field.set(pojo, innerAccessor.set(inner, valueToSet));
                return pojo;
            } catch (IllegalAccessException iaex) {
                // The Field class is transient and when deserializing its value we also make it
                // accessible
                throw new RuntimeException(
                        "This should not happen since we call setAccesssible(true) in readObject."
                                + " fields: "
                                + field
                                + " obj: "
                                + pojo);
            }
        }

        private void writeObject(ObjectOutputStream out)
                throws IOException, ClassNotFoundException {
            out.defaultWriteObject();
            FieldSerializer.serializeField(field, out);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            field = FieldSerializer.deserializeField(in);
        }
    }

    /**
     * There are two versions of ProductFieldAccessor, differing in whether there is an other
     * FieldAccessor nested inside. The no inner accessor version is probably a little faster.
     */
    static final class SimpleProductFieldAccessor<T, F> extends FieldAccessor<T, F> {

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

    static final class RecursiveProductFieldAccessor<T, R, F> extends FieldAccessor<T, F> {

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
