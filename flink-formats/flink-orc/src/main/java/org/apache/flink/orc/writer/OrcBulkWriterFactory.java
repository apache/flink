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

package org.apache.flink.orc.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.orc.vector.Vectorizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.impl.writer.WriterEncryptionVariant;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory that creates an ORC {@link BulkWriter}. The factory takes a user supplied {@link
 * Vectorizer} implementation to convert the element into an {@link
 * org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 *
 * @param <T> The type of element to write.
 */
@PublicEvolving
public class OrcBulkWriterFactory<T> implements BulkWriter.Factory<T> {

    private final Vectorizer<T> vectorizer;
    private final Properties writerProperties;
    private final Map<String, String> confMap;

    private OrcFile.WriterOptions writerOptions;

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer implementation.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    public OrcBulkWriterFactory(Vectorizer<T> vectorizer) {
        this(vectorizer, new Configuration());
    }

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    public OrcBulkWriterFactory(Vectorizer<T> vectorizer, Configuration configuration) {
        this(vectorizer, null, configuration);
    }

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration, ORC
     * writer properties.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     * @param writerProperties Properties that can be used in ORC WriterOptions.
     */
    public OrcBulkWriterFactory(
            Vectorizer<T> vectorizer, Properties writerProperties, Configuration configuration) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writerProperties = writerProperties;
        this.confMap = new HashMap<>();

        // Todo: Replace the Map based approach with a better approach
        for (Map.Entry<String, String> entry : configuration) {
            confMap.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream out) throws IOException {
        OrcFile.WriterOptions opts = getWriterOptions();
        PhysicalWriterImpl physicalWriter = new PhysicalWriterImpl(out, opts);
        opts.physicalWriter(physicalWriter);
        // The path of the Writer is not used to indicate the destination file
        // in this case since we have used a dedicated physical writer to write
        // to the give output stream directly. However, the path would be used as
        // the key of writer in the ORC memory manager, thus we need to make it unique.
        Path unusedPath = new Path(UUID.randomUUID().toString());
        WriterImpl writer = new WriterImpl(null, unusedPath, opts);

        // Obtaining encryption variant from Writer, and setting encryption variant for
        // physicalWriter.
        try {
            Field encryptionFiled = WriterImpl.class.getDeclaredField("encryption");
            encryptionFiled.setAccessible(true);
            WriterEncryptionVariant[] encryption =
                    (WriterEncryptionVariant[]) encryptionFiled.get(writer);
            physicalWriter.setEncryptionVariant(encryption);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(
                    "Can not access to the encryption field in Class org.apache.orc.impl.WriterImpl",
                    e);
        }
        return new OrcBulkWriter<>(vectorizer, writer);
    }

    @VisibleForTesting
    protected OrcFile.WriterOptions getWriterOptions() {
        if (null == writerOptions) {
            Configuration conf = new ThreadLocalClassLoaderConfiguration();
            for (Map.Entry<String, String> entry : confMap.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }

            writerOptions = OrcFile.writerOptions(writerProperties, conf);
            writerOptions.setSchema(this.vectorizer.getSchema());
        }

        return writerOptions;
    }
}
