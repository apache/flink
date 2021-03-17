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

package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

/**
 * Reduces {@link AdminProtos.AdminService.BlockingInterface} to interface necessary for WAL
 * replication ({@link AdminProtos.AdminService.BlockingInterface#replicateWALEntry(RpcController,
 * AdminProtos.ReplicateWALEntryRequest)}) to keep implementing class small.
 */
@Internal
public interface ReplicationTargetInterface extends AdminProtos.AdminService.BlockingInterface {

    @Override
    default AdminProtos.ClearCompactionQueuesResponse clearCompactionQueues(
            RpcController arg0, AdminProtos.ClearCompactionQueuesRequest arg1) {
        throw new UnsupportedOperationException(
                "Operation \"clearCompactionQueues\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.GetRegionInfoResponse getRegionInfo(
            RpcController controller, AdminProtos.GetRegionInfoRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getRegionInfo\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.GetStoreFileResponse getStoreFile(
            RpcController controller, AdminProtos.GetStoreFileRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getStoreFile\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.GetOnlineRegionResponse getOnlineRegion(
            RpcController controller, AdminProtos.GetOnlineRegionRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getOnlineRegion\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.OpenRegionResponse openRegion(
            RpcController controller, AdminProtos.OpenRegionRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"openRegion\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.WarmupRegionResponse warmupRegion(
            RpcController controller, AdminProtos.WarmupRegionRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"warmupRegion\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.CloseRegionResponse closeRegion(
            RpcController controller, AdminProtos.CloseRegionRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"closeRegion\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.FlushRegionResponse flushRegion(
            RpcController controller, AdminProtos.FlushRegionRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"flushRegion\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.CompactionSwitchResponse compactionSwitch(
            RpcController controller, AdminProtos.CompactionSwitchRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"compactionSwitch\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.CompactRegionResponse compactRegion(
            RpcController controller, AdminProtos.CompactRegionRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"compactRegion\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.ReplicateWALEntryResponse replay(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"replay\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.RollWALWriterResponse rollWALWriter(
            RpcController controller, AdminProtos.RollWALWriterRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"rollWALWriter\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.GetServerInfoResponse getServerInfo(
            RpcController controller, AdminProtos.GetServerInfoRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getServerInfo\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.StopServerResponse stopServer(
            RpcController controller, AdminProtos.StopServerRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"stopServer\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(
            RpcController controller, AdminProtos.UpdateFavoredNodesRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"updateFavoredNodes\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.UpdateConfigurationResponse updateConfiguration(
            RpcController controller, AdminProtos.UpdateConfigurationRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"updateConfiguration\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.GetRegionLoadResponse getRegionLoad(
            RpcController controller, AdminProtos.GetRegionLoadRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getRegionLoad\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.ClearRegionBlockCacheResponse clearRegionBlockCache(
            RpcController controller, AdminProtos.ClearRegionBlockCacheRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"clearRegionBlockCache\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.ExecuteProceduresResponse executeProcedures(
            RpcController controller, AdminProtos.ExecuteProceduresRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"executeProcedures\" not implemented in class " + getClass());
    }

    @Override
    default QuotaProtos.GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(
            RpcController arg0, QuotaProtos.GetSpaceQuotaSnapshotsRequest arg1) {
        throw new UnsupportedOperationException(
                "Operation \"getSpaceQuotaSnapshots\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.SlowLogResponses getSlowLogResponses(
            RpcController controller, AdminProtos.SlowLogResponseRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getSlowLogResponses\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.SlowLogResponses getLargeLogResponses(
            RpcController controller, AdminProtos.SlowLogResponseRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"getLargeLogResponses\" not implemented in class " + getClass());
    }

    @Override
    default AdminProtos.ClearSlowLogResponses clearSlowLogsResponses(
            RpcController controller, AdminProtos.ClearSlowLogResponseRequest request) {
        throw new UnsupportedOperationException(
                "Operation \"clearSlowLogsResponses\" not implemented in class " + getClass());
    }
}
