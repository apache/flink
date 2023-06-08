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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.util.ResourceLeakDetector;
import org.apache.flink.shaded.netty4.io.netty.util.ResourceLeakDetectorFactory;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * JUnit5 extension to fail with an assertion when Netty detects a resource leak (only with
 * <tt>ERROR</tt> logging enabled for
 * <tt>org.apache.flink.shaded.netty4.io.netty.util.ResourceLeakDetector</tt>).
 *
 * <p>This should be registered as a junit5 extension:
 * <pre>{@code
 * @literal @RegisterExtension
 *  public static final NettyLeakDetectionExtension LEAK_DETECTION = new NettyLeakDetectionExtension();
 * }</pre>
 */
public class NettyLeakDetectionExtension implements BeforeAllCallback, AfterAllCallback {
    @GuardedBy("refCountLock")
    private static ResourceLeakDetectorFactory previousLeakDetector;

    @GuardedBy("refCountLock")
    private static ResourceLeakDetector.Level previousLeakDetectorLevel;

    private static final Object refCountLock = new Object();
    private static int refCount = 0;

    public NettyLeakDetectionExtension() {
        assertThat(LoggerFactory.getLogger(ResourceLeakDetector.class).isErrorEnabled())
                .withFailMessage("Error logging must be enabled for the ResourceLeakDetector.")
                .isTrue();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        synchronized (refCountLock) {
            if (refCount == 0) {
                previousLeakDetector = ResourceLeakDetectorFactory.instance();
                previousLeakDetectorLevel = ResourceLeakDetector.getLevel();

                ++refCount;
                ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
                ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
                        new FailingResourceLeakDetectorFactory());
            }
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        synchronized (refCountLock) {
            --refCount;
            if (refCount == 0) {
                ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(previousLeakDetector);
                ResourceLeakDetector.setLevel(previousLeakDetectorLevel);
            }
        }
    }

    private static class FailingResourceLeakDetectorFactory extends ResourceLeakDetectorFactory {
        public <T> ResourceLeakDetector<T> newResourceLeakDetector(
                Class<T> resource, int samplingInterval, long maxActive) {
            return new FailingResourceLeakDetector<>(resource, samplingInterval);
        }
    }

    private static class FailingResourceLeakDetector<T> extends ResourceLeakDetector<T> {
        FailingResourceLeakDetector(Class<?> resourceType, int samplingInterval) {
            super(resourceType, samplingInterval);
        }

        @Override
        protected void reportTracedLeak(String resourceType, String records) {
            super.reportTracedLeak(resourceType, records);
            fail(
                    String.format(
                            "LEAK: %s.release() was not called before it's garbage-collected.%s",
                            resourceType, records));
        }

        @Override
        protected void reportUntracedLeak(String resourceType) {
            super.reportUntracedLeak(resourceType);
            fail(
                    String.format(
                            "LEAK: %s.release() was not called before it's garbage-collected.",
                            resourceType));
        }
    }
}
