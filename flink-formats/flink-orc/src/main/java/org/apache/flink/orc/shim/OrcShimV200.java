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

package org.apache.flink.orc.shim;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.orc.OrcFilters.Predicate;
import org.apache.flink.orc.vector.HiveOrcBatchWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcConf;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.apache.commons.lang3.reflect.ConstructorUtils.invokeConstructor;
import static org.apache.commons.lang3.reflect.MethodUtils.invokeExactMethod;
import static org.apache.commons.lang3.reflect.MethodUtils.invokeStaticMethod;

/** Shim orc for Hive version 2.0.0 and upper versions. */
public class OrcShimV200 implements OrcShim<VectorizedRowBatch> {

    private static final long serialVersionUID = 1L;

    private transient Method hasNextMethod;
    private transient Method nextBatchMethod;

    protected Reader createReader(Path path, Configuration conf) throws IOException {
        try {
            Class orcFileClass = Class.forName("org.apache.hadoop.hive.ql.io.orc.OrcFile");
            Object readerOptions = invokeStaticMethod(orcFileClass, "readerOptions", conf);

            Class readerClass = Class.forName("org.apache.hadoop.hive.ql.io.orc.ReaderImpl");
            //noinspection unchecked
            return (Reader) invokeConstructor(readerClass, path, readerOptions);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                | InstantiationException
                | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    protected RecordReader createRecordReader(Reader reader, Reader.Options options)
            throws IOException {
        try {
            return (RecordReader) invokeExactMethod(reader, "rowsOptions", options);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    protected Reader.Options readOrcConf(Reader.Options options, Configuration conf) {
        return options.useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
                .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf));
    }

    @Override
    public RecordReader createRecordReader(
            Configuration conf,
            TypeDescription schema,
            int[] selectedFields,
            List<Predicate> conjunctPredicates,
            org.apache.flink.core.fs.Path path,
            long splitStart,
            long splitLength)
            throws IOException {
        // open ORC file and create reader
        Path hPath = new Path(path.toUri());

        Reader orcReader = createReader(hPath, conf);

        // get offset and length for the stripes that start in the split
        Tuple2<Long, Long> offsetAndLength =
                getOffsetAndLengthForSplit(splitStart, splitLength, orcReader.getStripes());

        // create ORC row reader configuration
        Reader.Options options =
                readOrcConf(
                        new Reader.Options()
                                .schema(schema)
                                .range(offsetAndLength.f0, offsetAndLength.f1),
                        conf);

        // configure filters
        if (!conjunctPredicates.isEmpty()) {
            SearchArgument.Builder b = SearchArgumentFactory.newBuilder();
            b = b.startAnd();
            for (Predicate predicate : conjunctPredicates) {
                predicate.add(b);
            }
            b = b.end();
            options.searchArgument(b.build(), new String[] {});
        }

        // configure selected fields
        options.include(computeProjectionMask(schema, selectedFields));

        // create ORC row reader
        RecordReader orcRowsReader = createRecordReader(orcReader, options);

        // assign ids
        schema.getId();

        return orcRowsReader;
    }

    @Override
    public HiveOrcBatchWrapper createBatchWrapper(TypeDescription schema, int batchSize) {
        return new HiveOrcBatchWrapper(schema.createRowBatch(batchSize));
    }

    @Override
    public boolean nextBatch(RecordReader reader, VectorizedRowBatch rowBatch) throws IOException {
        try {
            if (hasNextMethod == null) {
                hasNextMethod =
                        Class.forName("org.apache.hadoop.hive.ql.io.orc.RecordReader")
                                .getMethod("hasNext");
                hasNextMethod.setAccessible(true);
            }
            if (nextBatchMethod == null) {
                nextBatchMethod =
                        RecordReader.class.getMethod("nextBatch", VectorizedRowBatch.class);
                nextBatchMethod.setAccessible(true);
            }
            boolean hasNext = (boolean) hasNextMethod.invoke(reader);
            if (hasNext) {
                nextBatchMethod.invoke(reader, rowBatch);
                return true;
            } else {
                return false;
            }
        } catch (IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException
                | ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @VisibleForTesting
    public static Tuple2<Long, Long> getOffsetAndLengthForSplit(
            long splitStart, long splitLength, List<StripeInformation> stripes) {
        long splitEnd = splitStart + splitLength;
        long readStart = Long.MAX_VALUE;
        long readEnd = Long.MIN_VALUE;

        for (StripeInformation s : stripes) {
            if (splitStart <= s.getOffset() && s.getOffset() < splitEnd) {
                // stripe starts in split, so it is included
                readStart = Math.min(readStart, s.getOffset());
                readEnd = Math.max(readEnd, s.getOffset() + s.getLength());
            }
        }

        if (readStart < Long.MAX_VALUE) {
            // at least one split is included
            return Tuple2.of(readStart, readEnd - readStart);
        } else {
            return Tuple2.of(0L, 0L);
        }
    }

    /**
     * Computes the ORC projection mask of the fields to include from the selected
     * fields.rowOrcInputFormat.nextRecord(null).
     *
     * @return The ORC projection mask.
     */
    public static boolean[] computeProjectionMask(TypeDescription schema, int[] selectedFields) {
        // mask with all fields of the schema
        boolean[] projectionMask = new boolean[schema.getMaximumId() + 1];
        // for each selected field
        for (int inIdx : selectedFields) {
            // set all nested fields of a selected field to true
            TypeDescription fieldSchema = schema.getChildren().get(inIdx);
            for (int i = fieldSchema.getId(); i <= fieldSchema.getMaximumId(); i++) {
                projectionMask[i] = true;
            }
        }
        return projectionMask;
    }
}
