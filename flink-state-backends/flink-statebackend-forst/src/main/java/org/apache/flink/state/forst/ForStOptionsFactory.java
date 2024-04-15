/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.annotation.Experimental;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.WriteOptions;

import java.util.Collection;

/**
 * A factory for {@link DBOptions} and {@link ColumnFamilyOptions} to be passed to the
 * ForStStateBackend. Options have to be created lazily by this factory, because the {@code Options}
 * class is not serializable and holds pointers to native code.
 */
@Experimental
public interface ForStOptionsFactory extends java.io.Serializable {

    /**
     * This method should set the additional options on top of the current options object. The
     * current options object may contain pre-defined options based on flags that have been
     * configured on the state backend.
     *
     * <p>It is important to set the options on the current object and return the result from the
     * setter methods, otherwise the pre-defined options may get lost.
     *
     * @param currentOptions The options object with the pre-defined options.
     * @param handlesToClose The collection to register newly created {@link
     *     org.rocksdb.RocksObject}s.
     * @return The options object on which the additional options are set.
     */
    DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose);

    /**
     * This method should set the additional options on top of the current options object. The
     * current options object may contain pre-defined options based on flags that have been
     * configured on the state backend.
     *
     * <p>It is important to set the options on the current object and return the result from the
     * setter methods, otherwise the pre-defined options may get lost.
     *
     * @param currentOptions The options object with the pre-defined options.
     * @param handlesToClose The collection to register newly created {@link
     *     org.rocksdb.RocksObject}s.
     * @return The options object on which the additional options are set.
     */
    ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose);

    /**
     * This method should enable certain ForSt metrics to be forwarded to Flink's metrics reporter.
     *
     * <p>Enabling these monitoring options may degrade ForSt performance and should be set with
     * care.
     *
     * @param nativeMetricOptions The options object with the pre-defined options.
     * @return The options object on which the additional options are set.
     */
    default ForStNativeMetricOptions createNativeMetricsOptions(
            ForStNativeMetricOptions nativeMetricOptions) {
        return nativeMetricOptions;
    }

    /**
     * This method should set the additional options on top of the current options object. The
     * current options object may contain pre-defined options based on flags that have been
     * configured on the state backend.
     *
     * <p>It is important to set the options on the current object and return the result from the
     * setter methods, otherwise the pre-defined options may get lost.
     *
     * @param currentOptions The options object with the pre-defined options.
     * @param handlesToClose The collection to register newly created {@link
     *     org.rocksdb.RocksObject}s.
     * @return The options object on which the additional options are set.
     */
    default WriteOptions createWriteOptions(
            WriteOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions;
    }

    /**
     * This method should set the additional options on top of the current options object. The
     * current options object may contain pre-defined options based on flags that have been
     * configured on the state backend.
     *
     * <p>It is important to set the options on the current object and return the result from the
     * setter methods, otherwise the pre-defined options may get lost.
     *
     * @param currentOptions The options object with the pre-defined options.
     * @param handlesToClose The collection to register newly created {@link
     *     org.rocksdb.RocksObject}s.
     * @return The options object on which the additional options are set.
     */
    default ReadOptions createReadOptions(
            ReadOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions;
    }
}
