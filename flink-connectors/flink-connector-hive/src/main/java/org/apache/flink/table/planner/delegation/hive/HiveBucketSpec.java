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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** A wrapper for Hive table bucketing information. */
public class HiveBucketSpec {
    private static final String BUCKET_NUM =
            CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + "hive.bucket.bucket_num";
    private static final String BUCKET_COL =
            CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + "hive.bucket.bucket_col";
    private static final String BUCKET_SORT_COL =
            CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + "hive.bucket.sort_col";
    private static final String BUCKET_SORT_ORDER =
            CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + "hive.bucket.sort_order";
    private static final String DELIMITER = ",";

    @Nullable private final Integer bucketNum;
    @Nullable private final List<String> bucketColumns;
    @Nullable private final Tuple2<List<String>, List<Integer>> columnNamesOrder;

    public HiveBucketSpec(
            @Nullable Integer bucketNum,
            @Nullable List<String> bucketColumns,
            @Nullable Tuple2<List<String>, List<Integer>> sortColumnNameOrders) {
        this.bucketNum = bucketNum;
        this.bucketColumns = bucketColumns;
        this.columnNamesOrder = sortColumnNameOrders;
    }

    public Optional<Integer> getBucketNum() {
        return Optional.ofNullable(bucketNum);
    }

    public Optional<List<String>> getBucketColumns() {
        return Optional.ofNullable(bucketColumns);
    }

    public Optional<Tuple2<List<String>, List<Integer>>> getColumnNamesOrder() {
        return Optional.ofNullable(columnNamesOrder);
    }

    public void dumpToProps(Map<String, String> props) {
        getBucketNum().ifPresent(n -> dumpBucketNumToProp(n, props));
        getBucketColumns().ifPresent(bucketCols -> dumpBucketColumnsToProp(bucketCols, props));
        getColumnNamesOrder().ifPresent(colOrder -> dumpSortColumnsToProp(colOrder, props));
    }

    public static void dumpBucketColumnsToProp(
            List<String> bucketColumns, Map<String, String> props) {
        props.put(BUCKET_COL, String.join(DELIMITER, bucketColumns));
    }

    public static void dumpBucketNumToProp(int bucketNum, Map<String, String> props) {
        props.put(BUCKET_NUM, String.valueOf(bucketNum));
    }

    public static void dumpSortColumnsToProp(
            Tuple2<List<String>, List<Integer>> columnNamesOrder, Map<String, String> props) {
        props.put(BUCKET_SORT_COL, String.join(DELIMITER, columnNamesOrder.f0));
        props.put(
                BUCKET_SORT_ORDER,
                String.join(
                        DELIMITER,
                        columnNamesOrder.f1.stream()
                                .map(String::valueOf)
                                .collect(Collectors.joining(DELIMITER))));
    }

    public static HiveBucketSpec extractBucketSpec(Map<String, String> props) {
        Integer bucketNum = null;
        String bucketNumProp = props.remove(BUCKET_NUM);
        if (bucketNumProp != null) {
            bucketNum = Integer.parseInt(bucketNumProp);
        }
        List<String> bucketColumns =
                extractListValueFromProp(props, BUCKET_COL, Function.identity());
        List<String> sortColumnNames =
                extractListValueFromProp(props, BUCKET_SORT_COL, Function.identity());
        List<Integer> sortColumnOrder =
                extractListValueFromProp(props, BUCKET_SORT_ORDER, Integer::valueOf);
        Tuple2<List<String>, List<Integer>> columnNamesOrder = null;
        if (sortColumnNames != null && sortColumnOrder != null) {
            columnNamesOrder = new Tuple2<>(sortColumnNames, sortColumnOrder);
        }
        return new HiveBucketSpec(bucketNum, bucketColumns, columnNamesOrder);
    }

    private static <T> List<T> extractListValueFromProp(
            Map<String, String> props,
            String key,
            Function<? super String, ? extends T> elementConverter) {
        String value = props.remove(key);
        if (value == null) {
            return null;
        }
        if (value.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(DELIMITER))
                .map(elementConverter)
                .collect(Collectors.toList());
    }
}
