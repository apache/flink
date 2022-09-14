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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import static org.apache.hadoop.hive.ql.exec.SerializationUtilities.borrowKryo;
import static org.apache.hadoop.hive.ql.exec.SerializationUtilities.releaseKryo;

/**
 * A wrapper of Hive functions that instantiate function instances and ser/de function instance
 * cross process boundary.
 *
 * @param <UDFType> The type of UDF.
 */
@Internal
public class HiveFunctionWrapper<UDFType> implements Serializable {

    public static final long serialVersionUID = 393313529306818205L;

    private final Class<UDFType> functionClz;
    // a field to hold the bytes serialized for the UDF.
    // we sometimes need to hold it in case of some serializable UDF will contain
    // additional information such as Hive's GenericUDFMacro and if we construct the UDF directly by
    // getUDFClass#newInstance, the information will be missed.
    private byte[] udfSerializedBytes;

    private transient UDFType instance = null;

    @SuppressWarnings("unchecked")
    public HiveFunctionWrapper(Class<?> functionClz) {
        this.functionClz = (Class<UDFType>) functionClz;
    }

    /**
     * Create a HiveFunctionWrapper with a UDF instance. In this constructor, the instance will be
     * serialized to string and held on in the HiveFunctionWrapper.
     */
    public HiveFunctionWrapper(Class<?> functionClz, UDFType serializableInstance) {
        this(functionClz);
        Preconditions.checkArgument(
                serializableInstance.getClass().getName().equals(getUDFClassName()),
                String.format(
                        "Expect the UDF is instance of %s, but is instance of %s.",
                        getUDFClassName(), serializableInstance.getClass().getName()));
        Preconditions.checkArgument(
                serializableInstance instanceof Serializable,
                String.format(
                        "The UDF %s should be an instance of Serializable.",
                        serializableInstance.getClass().getName()));
        // we need to use the SerializationUtilities#serializeObject to serialize UDF for the UDF
        // may not be serialized by Java serializer
        this.udfSerializedBytes = serializeObjectToKryo((Serializable) serializableInstance);
    }

    private static byte[] serializeObjectToKryo(Serializable object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        Kryo kryo = borrowKryo();
        try {
            kryo.writeObject(output, object);
        } finally {
            releaseKryo(kryo);
        }
        output.close();
        return baos.toByteArray();
    }

    private static <T extends Serializable> T deserializeObjectFromKryo(
            byte[] bytes, Class<T> clazz) {
        Input inp = new Input(new ByteArrayInputStream(bytes));
        Kryo kryo = borrowKryo();
        ClassLoader oldClassLoader = kryo.getClassLoader();
        kryo.setClassLoader(clazz.getClassLoader());
        T func;

        try {
            func = kryo.readObject(inp, clazz);
        } finally {
            kryo.setClassLoader(oldClassLoader);
            releaseKryo(kryo);
        }

        inp.close();
        return func;
    }

    /**
     * Instantiate a Hive function instance.
     *
     * @return a Hive function instance
     */
    public UDFType createFunction() {
        if (udfSerializedBytes != null) {
            // deserialize the string to udf instance
            return deserializeUDF();
        } else if (instance != null) {
            return instance;
        } else {
            UDFType func;
            try {
                func = functionClz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new FlinkHiveUDFException(
                        String.format("Failed to create function from %s", functionClz.getName()),
                        e);
            }

            if (!(func instanceof UDF)) {
                // We cache the function if it is not the Simple UDF,
                // as we always have to create new instance for Simple UDF.
                instance = func;
            }

            return func;
        }
    }

    /**
     * Get class name of the Hive function.
     *
     * @return class name of the Hive function
     */
    public String getUDFClassName() {
        return functionClz.getName();
    }

    public Class<UDFType> getUDFClass() {
        return functionClz;
    }

    /**
     * Deserialize UDF used the udfSerializedString held on.
     *
     * @return the UDF deserialized
     */
    @SuppressWarnings("unchecked")
    private UDFType deserializeUDF() {
        return (UDFType)
                deserializeObjectFromKryo(udfSerializedBytes, (Class<Serializable>) getUDFClass());
    }
}
