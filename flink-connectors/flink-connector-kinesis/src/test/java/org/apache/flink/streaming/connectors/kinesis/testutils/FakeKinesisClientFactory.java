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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeLimitsRequest;
import com.amazonaws.services.kinesis.model.DescribeLimitsResult;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ListStreamConsumersRequest;
import com.amazonaws.services.kinesis.model.ListStreamConsumersResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.MergeShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.SplitShardResult;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.amazonaws.services.kinesis.model.UpdateStreamModeRequest;
import com.amazonaws.services.kinesis.model.UpdateStreamModeResult;
import com.amazonaws.services.kinesis.waiters.AmazonKinesisWaiters;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Factory for different kinds of fake Amazon Kinesis Client behaviours using the {@link
 * KinesisProxyInterface} interface.
 */
public class FakeKinesisClientFactory {

    public static AmazonKinesis resourceNotFoundWhenGettingShardIterator(
            String streamName, List<String> shardIds) {
        return new AmazonKinesis() {

            @Override
            public GetShardIteratorResult getShardIterator(
                    GetShardIteratorRequest getShardIteratorRequest) {
                if (getShardIteratorRequest.getStreamName().equals(streamName)
                        && shardIds.contains(getShardIteratorRequest.getShardId())) {
                    return new GetShardIteratorResult().withShardIterator("fakeShardIterator");
                }

                final ResourceNotFoundException ex =
                        new ResourceNotFoundException(
                                "Requested resource not found: Shard does not exist");
                ex.setErrorType(AmazonServiceException.ErrorType.Client);

                throw ex;
            }

            @Override
            public void setEndpoint(String s) {}

            @Override
            public void setRegion(Region region) {}

            @Override
            public AddTagsToStreamResult addTagsToStream(
                    AddTagsToStreamRequest addTagsToStreamRequest) {
                return null;
            }

            @Override
            public CreateStreamResult createStream(CreateStreamRequest createStreamRequest) {
                return null;
            }

            @Override
            public CreateStreamResult createStream(String s, Integer integer) {
                return null;
            }

            @Override
            public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(
                    DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
                return null;
            }

            @Override
            public DeleteStreamResult deleteStream(DeleteStreamRequest deleteStreamRequest) {
                return null;
            }

            @Override
            public DeleteStreamResult deleteStream(String s) {
                return null;
            }

            @Override
            public DeregisterStreamConsumerResult deregisterStreamConsumer(
                    DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
                return null;
            }

            @Override
            public DescribeLimitsResult describeLimits(
                    DescribeLimitsRequest describeLimitsRequest) {
                return null;
            }

            @Override
            public DescribeStreamResult describeStream(
                    DescribeStreamRequest describeStreamRequest) {
                return null;
            }

            @Override
            public DescribeStreamResult describeStream(String s) {
                return null;
            }

            @Override
            public DescribeStreamResult describeStream(String s, String s1) {
                return null;
            }

            @Override
            public DescribeStreamResult describeStream(String s, Integer integer, String s1) {
                return null;
            }

            @Override
            public DescribeStreamConsumerResult describeStreamConsumer(
                    DescribeStreamConsumerRequest describeStreamConsumerRequest) {
                return null;
            }

            @Override
            public DescribeStreamSummaryResult describeStreamSummary(
                    DescribeStreamSummaryRequest describeStreamSummaryRequest) {
                return null;
            }

            @Override
            public DisableEnhancedMonitoringResult disableEnhancedMonitoring(
                    DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
                return null;
            }

            @Override
            public EnableEnhancedMonitoringResult enableEnhancedMonitoring(
                    EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
                return null;
            }

            @Override
            public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
                return null;
            }

            @Override
            public GetShardIteratorResult getShardIterator(String s, String s1, String s2) {
                return null;
            }

            @Override
            public GetShardIteratorResult getShardIterator(
                    String s, String s1, String s2, String s3) {
                return null;
            }

            @Override
            public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(
                    IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
                return null;
            }

            @Override
            public ListShardsResult listShards(ListShardsRequest listShardsRequest) {
                return null;
            }

            @Override
            public ListStreamConsumersResult listStreamConsumers(
                    ListStreamConsumersRequest listStreamConsumersRequest) {
                return null;
            }

            @Override
            public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
                return null;
            }

            @Override
            public ListStreamsResult listStreams() {
                return null;
            }

            @Override
            public ListStreamsResult listStreams(String s) {
                return null;
            }

            @Override
            public ListStreamsResult listStreams(Integer integer, String s) {
                return null;
            }

            @Override
            public ListTagsForStreamResult listTagsForStream(
                    ListTagsForStreamRequest listTagsForStreamRequest) {
                return null;
            }

            @Override
            public MergeShardsResult mergeShards(MergeShardsRequest mergeShardsRequest) {
                return null;
            }

            @Override
            public MergeShardsResult mergeShards(String s, String s1, String s2) {
                return null;
            }

            @Override
            public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
                return null;
            }

            @Override
            public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1) {
                return null;
            }

            @Override
            public PutRecordResult putRecord(
                    String s, ByteBuffer byteBuffer, String s1, String s2) {
                return null;
            }

            @Override
            public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
                return null;
            }

            @Override
            public RegisterStreamConsumerResult registerStreamConsumer(
                    RegisterStreamConsumerRequest registerStreamConsumerRequest) {
                return null;
            }

            @Override
            public RemoveTagsFromStreamResult removeTagsFromStream(
                    RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
                return null;
            }

            @Override
            public SplitShardResult splitShard(SplitShardRequest splitShardRequest) {
                return null;
            }

            @Override
            public SplitShardResult splitShard(String s, String s1, String s2) {
                return null;
            }

            @Override
            public StartStreamEncryptionResult startStreamEncryption(
                    StartStreamEncryptionRequest startStreamEncryptionRequest) {
                return null;
            }

            @Override
            public StopStreamEncryptionResult stopStreamEncryption(
                    StopStreamEncryptionRequest stopStreamEncryptionRequest) {
                return null;
            }

            @Override
            public UpdateShardCountResult updateShardCount(
                    UpdateShardCountRequest updateShardCountRequest) {
                return null;
            }

            @Override
            public UpdateStreamModeResult updateStreamMode(
                    UpdateStreamModeRequest updateStreamModeRequest) {
                return null;
            }

            @Override
            public void shutdown() {}

            @Override
            public ResponseMetadata getCachedResponseMetadata(
                    AmazonWebServiceRequest amazonWebServiceRequest) {
                return null;
            }

            @Override
            public AmazonKinesisWaiters waiters() {
                return null;
            }
        };
    }
}
