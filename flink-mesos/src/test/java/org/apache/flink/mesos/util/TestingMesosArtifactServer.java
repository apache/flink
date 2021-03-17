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

package org.apache.flink.mesos.util;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.function.BiFunction;
import java.util.function.Function;

import scala.Option;

/** Testing implementation of {@link MesosArtifactServer}. */
public class TestingMesosArtifactServer implements MesosArtifactServer {
    private final BiFunction<Path, Path, URL> addPathFunction;
    private final Runnable stopRunnable;
    private final Function<Path, Option<URL>> resolveFunction;

    private TestingMesosArtifactServer(
            final BiFunction<Path, Path, URL> addPathFunction,
            final Runnable stopRunnable,
            final Function<Path, Option<URL>> resolveFunction) {
        this.addPathFunction = addPathFunction;
        this.stopRunnable = stopRunnable;
        this.resolveFunction = resolveFunction;
    }

    @Override
    public URL addPath(Path path, Path remoteFile) {
        return addPathFunction.apply(path, remoteFile);
    }

    @Override
    public void stop() {
        stopRunnable.run();
    }

    @Override
    public Option<URL> resolve(Path remoteFile) {
        return resolveFunction.apply(remoteFile);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link TestingMesosArtifactServer}. */
    public static class Builder {
        private BiFunction<Path, Path, URL> addPathFunction = (ignore1, ignore2) -> null;
        private Runnable stopRunnable = () -> {};
        private Function<Path, Option<URL>> resolveFunction = (ignore) -> null;

        private Builder() {}

        public Builder setAddPathFunction(BiFunction<Path, Path, URL> addPathFunction) {
            this.addPathFunction = Preconditions.checkNotNull(addPathFunction);
            return this;
        }

        public Builder setStopRunnable(Runnable stopRunnable) {
            this.stopRunnable = Preconditions.checkNotNull(stopRunnable);
            return this;
        }

        public Builder setResolveFunction(Function<Path, Option<URL>> resolveFunction) {
            this.resolveFunction = Preconditions.checkNotNull(resolveFunction);
            return this;
        }

        public TestingMesosArtifactServer build() {
            return new TestingMesosArtifactServer(addPathFunction, stopRunnable, resolveFunction);
        }
    }
}
