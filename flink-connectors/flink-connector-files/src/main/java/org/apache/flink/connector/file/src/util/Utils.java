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

package org.apache.flink.connector.file.src.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

/** Miscellaneous utilities for the file source. */
@PublicEvolving
public final class Utils {

    /**
     * Runs the given {@code SupplierWithException} (a piece of code producing a result). If an
     * exception happens during that, the given closable is quietly closed.
     */
    public static <E> E doWithCleanupOnException(
            final Closeable toCleanUp, final SupplierWithException<E, IOException> code)
            throws IOException {

        try {
            return code.get();
        } catch (Throwable t) {
            IOUtils.closeQuietly(toCleanUp);
            ExceptionUtils.rethrowIOException(t);
            return null; // silence the compiler
        }
    }

    /**
     * Runs the given {@code Runnable}. If an exception happens during that, the given closable is
     * quietly closed.
     */
    public static void doWithCleanupOnException(
            final Closeable toCleanUp, final ThrowingRunnable<IOException> code)
            throws IOException {

        doWithCleanupOnException(
                toCleanUp,
                (SupplierWithException<Void, IOException>)
                        () -> {
                            code.run();
                            return null;
                        });
    }

    /**
     * Performs the given action for each remaining element in {@link BulkFormat.Reader} until all
     * elements have been processed or the action throws an exception.
     */
    public static <T> void forEachRemaining(
            final BulkFormat.Reader<T> reader, final Consumer<? super T> action)
            throws IOException {
        BulkFormat.RecordIterator<T> batch;
        RecordAndPosition<T> record;

        try {
            while ((batch = reader.readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record.getRecord());
                }
                batch.releaseBatch();
            }
        } finally {
            reader.close();
        }
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    private Utils() {}
}
