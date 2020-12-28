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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link FileOutputFormat} for Avro records.
 *
 * @param <E>
 */
public class AvroOutputFormat<E> extends FileOutputFormat<E> implements Serializable {

    /** Wrapper which encapsulates the supported codec and a related serialization byte. */
    public enum Codec {
        NULL((byte) 0, CodecFactory.nullCodec()),
        SNAPPY((byte) 1, CodecFactory.snappyCodec()),
        BZIP2((byte) 2, CodecFactory.bzip2Codec()),
        DEFLATE((byte) 3, CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL)),
        XZ((byte) 4, CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL));

        private byte codecByte;

        private CodecFactory codecFactory;

        Codec(final byte codecByte, final CodecFactory codecFactory) {
            this.codecByte = codecByte;
            this.codecFactory = codecFactory;
        }

        private byte getCodecByte() {
            return codecByte;
        }

        private CodecFactory getCodecFactory() {
            return codecFactory;
        }

        private static Codec forCodecByte(byte codecByte) {
            for (final Codec codec : Codec.values()) {
                if (codec.getCodecByte() == codecByte) {
                    return codec;
                }
            }
            throw new IllegalArgumentException("no codec for codecByte: " + codecByte);
        }
    }

    private static final long serialVersionUID = 1L;

    private final Class<E> avroValueType;

    private transient Schema userDefinedSchema = null;

    private transient Codec codec = null;

    private transient DataFileWriter<E> dataFileWriter;

    public AvroOutputFormat(Path filePath, Class<E> type) {
        super(filePath);
        this.avroValueType = type;
    }

    public AvroOutputFormat(Class<E> type) {
        this.avroValueType = type;
    }

    @Override
    protected String getDirectoryFileName(int taskNumber) {
        return super.getDirectoryFileName(taskNumber) + ".avro";
    }

    public void setSchema(Schema schema) {
        this.userDefinedSchema = schema;
    }

    /**
     * Set avro codec for compression.
     *
     * @param codec avro codec.
     */
    public void setCodec(final Codec codec) {
        this.codec = checkNotNull(codec, "codec can not be null");
    }

    @Override
    public void writeRecord(E record) throws IOException {
        dataFileWriter.append(record);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);

        DatumWriter<E> datumWriter;
        Schema schema;
        if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(avroValueType)) {
            datumWriter = new SpecificDatumWriter<E>(avroValueType);
            try {
                schema =
                        ((org.apache.avro.specific.SpecificRecordBase) avroValueType.newInstance())
                                .getSchema();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e.getMessage());
            }
        } else if (org.apache.avro.generic.GenericRecord.class.isAssignableFrom(avroValueType)) {
            if (userDefinedSchema == null) {
                throw new IllegalStateException("Schema must be set when using Generic Record");
            }
            datumWriter = new GenericDatumWriter<E>(userDefinedSchema);
            schema = userDefinedSchema;
        } else {
            datumWriter = new ReflectDatumWriter<E>(avroValueType);
            schema = ReflectData.get().getSchema(avroValueType);
        }
        dataFileWriter = new DataFileWriter<E>(datumWriter);
        if (codec != null) {
            dataFileWriter.setCodec(codec.getCodecFactory());
        }
        if (userDefinedSchema == null) {
            dataFileWriter.create(schema, stream);
        } else {
            dataFileWriter.create(userDefinedSchema, stream);
        }
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        if (codec != null) {
            out.writeByte(codec.getCodecByte());
        } else {
            out.writeByte(-1);
        }

        if (userDefinedSchema != null) {
            byte[] json = userDefinedSchema.toString().getBytes(ConfigConstants.DEFAULT_CHARSET);
            out.writeInt(json.length);
            out.write(json);
        } else {
            out.writeInt(0);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        byte codecByte = in.readByte();
        if (codecByte >= 0) {
            setCodec(Codec.forCodecByte(codecByte));
        }

        int length = in.readInt();
        if (length != 0) {
            byte[] json = new byte[length];
            in.readFully(json);

            Schema schema =
                    new Schema.Parser().parse(new String(json, ConfigConstants.DEFAULT_CHARSET));
            setSchema(schema);
        }
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.flush();
        dataFileWriter.close();
        super.close();
    }
}
