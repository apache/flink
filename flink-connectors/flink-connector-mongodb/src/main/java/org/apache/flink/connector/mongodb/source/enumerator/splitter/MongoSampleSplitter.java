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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MAX_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.BSON_MIN_KEY;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;

/**
 * Sample Partitioner
 *
 * <p>Samples the collection to generate partitions.
 *
 * <p>Uses the average document size to split the collection into average sized chunks
 *
 * <p>The partitioner samples the collection, projects and sorts by the partition fields. Then uses
 * every {@code samplesPerPartition} as the value to use to calculate the partition boundaries.
 *
 * <ul>
 *   <li>scan.partition.size: The average size (MB) for each partition. Note: Uses the average
 *       document size to determine the number of documents per partition so may not be even.
 *       Defaults to: 64mb.
 *   <li>scan.partition.samples: The number of samples to take per partition. Defaults to: 10. The
 *       total number of samples taken is calculated as: {@code samples per partition * (count /
 *       number of documents per partition)}.
 * </ul>
 */
@Internal
public class MongoSampleSplitter implements MongoSplitters.MongoSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSampleSplitter.class);

    public static final MongoSampleSplitter INSTANCE = new MongoSampleSplitter();

    private MongoSampleSplitter() {}

    @Override
    public Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoReadOptions readOptions = splitContext.getReadOptions();
        MongoNamespace namespace = splitContext.getMongoNamespace();

        long count = splitContext.getCount();
        long partitionSizeInBytes = readOptions.getPartitionSize().getBytes();
        int samplesPerPartition = readOptions.getSamplesPerPartition();

        long avgObjSizeInBytes = splitContext.getAvgObjSize();
        long numDocumentsPerPartition = partitionSizeInBytes / avgObjSizeInBytes;

        if (numDocumentsPerPartition >= count) {
            LOG.info(
                    "Fewer documents ({}) than the number of documents per partition ({}), fallback a SingleSplitter.",
                    count,
                    numDocumentsPerPartition);
            return MongoSingleSplitter.INSTANCE.split(splitContext);
        }

        int numberOfSamples =
                (int) Math.ceil((samplesPerPartition * count * 1.0d) / numDocumentsPerPartition);

        List<BsonDocument> samples =
                splitContext
                        .getMongoCollection()
                        .aggregate(
                                Arrays.asList(
                                        Aggregates.sample(numberOfSamples),
                                        Aggregates.project(Projections.include(ID_FIELD)),
                                        Aggregates.sort(Sorts.ascending(ID_FIELD))))
                        .allowDiskUse(true)
                        .into(new ArrayList<>());

        List<MongoScanSourceSplit> sourceSplits = new ArrayList<>();
        BsonDocument min = new BsonDocument(ID_FIELD, BSON_MIN_KEY);
        int splitNum = 0;
        for (int i = 0; i < samples.size(); i++) {
            if (i % samplesPerPartition == 0 || i == samples.size() - 1) {
                sourceSplits.add(createSplit(namespace, splitNum++, min, samples.get(i)));
                min = samples.get(i);
            }
        }

        // Complete right bound: (upper of last, maxKey)
        sourceSplits.add(
                createSplit(namespace, splitNum, min, new BsonDocument(ID_FIELD, BSON_MAX_KEY)));

        return sourceSplits;
    }

    private MongoScanSourceSplit createSplit(
            MongoNamespace ns, int index, BsonDocument min, BsonDocument max) {
        return new MongoScanSourceSplit(
                String.format("%s_%d", ns, index),
                ns.getDatabaseName(),
                ns.getCollectionName(),
                min,
                max,
                ID_HINT);
    }
}
