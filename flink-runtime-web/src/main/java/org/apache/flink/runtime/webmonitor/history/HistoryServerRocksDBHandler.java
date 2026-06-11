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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.runtime.rest.handler.util.HandlerUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * HistoryServer handler based on the RocksDB storage backend.
 *
 * <p>JSON data will be responded with the JSON string.
 *
 * <p>Static files will be responded with the file.
 */
@ChannelHandler.Sharable
public class HistoryServerRocksDBHandler extends AbstractHistoryServerHandler<String> {

    public HistoryServerRocksDBHandler(RocksDBArchiveStorage rocksDBArchiveStorage, File rootPath)
            throws IOException {
        super(rocksDBArchiveStorage, rootPath);
    }

    @Override
    protected void respondWithResource(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath, String resource)
            throws Exception {
        HandlerUtils.sendResponse(
                ctx, request, resource, HttpResponseStatus.OK, Collections.emptyMap());
    }
}
