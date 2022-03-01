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

package org.apache.flink.table.runtime.arrow.serializers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The base class ArrowSerializer which will serialize/deserialize RowType data to/from arrow bytes.
 */
@Internal
public final class ArrowSerializer {

    static {
        ArrowUtils.checkArrowUsable();
    }

    /** The input RowType. */
    protected final RowType inputType;

    /** The output RowType. */
    protected final RowType outputType;

    /** Allocator which is used for byte buffer allocation. */
    private transient BufferAllocator allocator;

    /** Reader which is responsible for deserialize the Arrow format data to the Flink rows. */
    private transient ArrowReader arrowReader;

    /**
     * Reader which is responsible for convert the execution result from byte array to arrow format.
     */
    private transient ArrowStreamReader arrowStreamReader;

    /**
     * Container that holds a set of vectors for the input elements to be sent to the Python worker.
     */
    transient VectorSchemaRoot rootWriter;

    /** Writer which is responsible for serialize the input elements to arrow format. */
    private transient ArrowWriter<RowData> arrowWriter;

    /** Writer which is responsible for convert the arrow format data into byte array. */
    private transient ArrowStreamWriter arrowStreamWriter;

    /** Reusable InputStream used to holding the execution results to be deserialized. */
    private transient InputStream bais;

    /** Reusable OutputStream used to holding the serialized input elements. */
    private transient OutputStream baos;

    public ArrowSerializer(RowType inputType, RowType outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public void open(InputStream bais, OutputStream baos) throws Exception {
        this.bais = bais;
        this.baos = baos;
        allocator = ArrowUtils.getRootAllocator().newChildAllocator("allocator", 0, Long.MAX_VALUE);
        arrowStreamReader = new ArrowStreamReader(bais, allocator);

        rootWriter = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(inputType), allocator);
        arrowWriter = createArrowWriter();
        arrowStreamWriter = new ArrowStreamWriter(rootWriter, null, baos);
        arrowStreamWriter.start();
    }

    public int load() throws IOException {
        arrowStreamReader.loadNextBatch();
        VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
        if (arrowReader == null) {
            arrowReader = createArrowReader(root);
        }
        return root.getRowCount();
    }

    public RowData read(int i) {
        return arrowReader.read(i);
    }

    public void write(RowData element) {
        arrowWriter.write(element);
    }

    public void close() throws Exception {
        arrowStreamWriter.end();
        arrowStreamReader.close();
        rootWriter.close();
        allocator.close();
    }

    /** Creates an {@link ArrowWriter}. */
    public ArrowWriter<RowData> createArrowWriter() {
        return ArrowUtils.createRowDataArrowWriter(rootWriter, inputType);
    }

    public ArrowReader createArrowReader(VectorSchemaRoot root) {
        return ArrowUtils.createArrowReader(root, outputType);
    }

    /**
     * Forces to finish the processing of the current batch of elements. It will serialize the batch
     * of elements into one arrow batch.
     */
    public void finishCurrentBatch() throws Exception {
        arrowWriter.finish();
        arrowStreamWriter.writeBatch();
        arrowWriter.reset();
    }

    public void resetReader() throws IOException {
        arrowReader = null;
        arrowStreamReader.close();
        arrowStreamReader = new ArrowStreamReader(bais, allocator);
    }

    public void resetWriter() throws IOException {
        arrowStreamWriter = new ArrowStreamWriter(rootWriter, null, baos);
        arrowStreamWriter.start();
    }
}
