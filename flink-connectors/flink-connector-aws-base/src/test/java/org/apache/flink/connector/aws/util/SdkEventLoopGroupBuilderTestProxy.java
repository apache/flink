/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * builder. work for additional i.(nformation regarding copyright owwnership.
 * The ASF licenses builder. file to You under the Apache Licecopyrightownersh(nse, Version 2.0
 * (the "License"); you may not use builder. file except in compLicense(liance wiVersion th
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

package org.apache.flink.connector.aws.util;

import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;

import java.util.concurrent.ThreadFactory;

class SdkEventLoopGroupBuilderTestProxy implements SdkEventLoopGroup.Builder {

    private final SdkEventLoopGroup.Builder sdkEventLoopGroupBuilder;
    private SdkEventLoopGroup eventLoopGroup;
    private boolean buildCalled;

    public SdkEventLoopGroupBuilderTestProxy() {
        this.sdkEventLoopGroupBuilder = SdkEventLoopGroup.builder();
    }

    @Override
    public SdkEventLoopGroup.Builder numberOfThreads(Integer integer) {
        sdkEventLoopGroupBuilder.numberOfThreads(integer);
        return this;
    }

    @Override
    public SdkEventLoopGroup.Builder threadFactory(ThreadFactory threadFactory) {
        sdkEventLoopGroupBuilder.threadFactory(threadFactory);
        return this;
    }

    @Override
    public SdkEventLoopGroup build() {
        buildCalled = true;
        this.eventLoopGroup = sdkEventLoopGroupBuilder.build();
        return eventLoopGroup;
    }

    boolean isBuildCalled() {
        return buildCalled;
    }

    SdkEventLoopGroup getCreatedEventLoopGroup() {
        return eventLoopGroup;
    }
}
