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

package org.apache.flink.mesos.runtime.clusterframework.store;

import org.apache.flink.util.Preconditions;

import org.apache.mesos.Protos;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import scala.Option;

/** Testing implementation for {@link MesosWorkerStore}. */
public class TestingMesosWorkerStore implements MesosWorkerStore {

    private final Consumer<Boolean> stopConsumer;
    private final Supplier<Option<Protos.FrameworkID>> getFrameworkIDSupplier;
    private final Consumer<Option<Protos.FrameworkID>> setFrameworkIDConsumer;
    private final Supplier<List<Worker>> recoverWorkersSupplier;
    private final Supplier<Protos.TaskID> newTaskIDSupplier;
    private final Consumer<Worker> putWorkerConsumer;
    private final Function<Protos.TaskID, Boolean> removeWorkerFunction;

    private TestingMesosWorkerStore(
            final Consumer<Boolean> stopConsumer,
            final Supplier<Option<Protos.FrameworkID>> getFrameworkIDSupplier,
            final Consumer<Option<Protos.FrameworkID>> setFrameworkIDConsumer,
            final Supplier<List<Worker>> recoverWorkersSupplier,
            final Supplier<Protos.TaskID> newTaskIDSupplier,
            final Consumer<Worker> putWorkerConsumer,
            final Function<Protos.TaskID, Boolean> removeWorkerFunction) {
        this.stopConsumer = stopConsumer;
        this.getFrameworkIDSupplier = getFrameworkIDSupplier;
        this.setFrameworkIDConsumer = setFrameworkIDConsumer;
        this.recoverWorkersSupplier = recoverWorkersSupplier;
        this.newTaskIDSupplier = newTaskIDSupplier;
        this.putWorkerConsumer = putWorkerConsumer;
        this.removeWorkerFunction = removeWorkerFunction;
    }

    @Override
    public void start() {
        // noop
    }

    @Override
    public void stop(boolean cleanup) {
        stopConsumer.accept(cleanup);
    }

    @Override
    public Option<Protos.FrameworkID> getFrameworkID() {
        return getFrameworkIDSupplier.get();
    }

    @Override
    public void setFrameworkID(Option<Protos.FrameworkID> frameworkID) {
        setFrameworkIDConsumer.accept(frameworkID);
    }

    @Override
    public List<Worker> recoverWorkers() {
        return recoverWorkersSupplier.get();
    }

    @Override
    public Protos.TaskID newTaskID() {
        return newTaskIDSupplier.get();
    }

    @Override
    public void putWorker(Worker worker) {
        putWorkerConsumer.accept(worker);
    }

    @Override
    public boolean removeWorker(Protos.TaskID taskID) {
        return removeWorkerFunction.apply(taskID);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link TestingMesosWorkerStore}. */
    public static class Builder {
        private Consumer<Boolean> stopConsumer = (ignore) -> {};
        private Supplier<Option<Protos.FrameworkID>> getFrameworkIDSupplier =
                () ->
                        Option.apply(
                                Protos.FrameworkID.newBuilder()
                                        .setValue("testing-framework")
                                        .build());
        private Consumer<Option<Protos.FrameworkID>> setFrameworkIDConsumer = (ignore) -> {};
        private Supplier<List<Worker>> recoverWorkersSupplier = Collections::emptyList;
        private Supplier<Protos.TaskID> newTaskIDSupplier =
                () -> Protos.TaskID.newBuilder().setValue("testing-task").build();
        private Consumer<Worker> putWorkerConsumer = (ignore) -> {};
        private Function<Protos.TaskID, Boolean> removeWorkerFunction = (ignore) -> true;

        private Builder() {}

        public Builder setStopConsumer(Consumer<Boolean> stopConsumer) {
            this.stopConsumer = Preconditions.checkNotNull(stopConsumer);
            return this;
        }

        public Builder setGetFrameworkIDSupplier(
                Supplier<Option<Protos.FrameworkID>> getFrameworkIDSupplier) {
            this.getFrameworkIDSupplier = Preconditions.checkNotNull(getFrameworkIDSupplier);
            return this;
        }

        public Builder setSetFrameworkIDConsumer(
                Consumer<Option<Protos.FrameworkID>> setFrameworkIDConsumer) {
            this.setFrameworkIDConsumer = Preconditions.checkNotNull(setFrameworkIDConsumer);
            return this;
        }

        public Builder setRecoverWorkersSupplier(Supplier<List<Worker>> recoverWorkersSupplier) {
            this.recoverWorkersSupplier = Preconditions.checkNotNull(recoverWorkersSupplier);
            return this;
        }

        public Builder setNewTaskIDSupplier(Supplier<Protos.TaskID> newTaskIDSupplier) {
            this.newTaskIDSupplier = Preconditions.checkNotNull(newTaskIDSupplier);
            return this;
        }

        public Builder setPutWorkerConsumer(Consumer<Worker> putWorkerConsumer) {
            this.putWorkerConsumer = Preconditions.checkNotNull(putWorkerConsumer);
            return this;
        }

        public Builder setRemoveWorkerFunction(
                Function<Protos.TaskID, Boolean> removeWorkerFunction) {
            this.removeWorkerFunction = Preconditions.checkNotNull(removeWorkerFunction);
            return this;
        }

        public TestingMesosWorkerStore build() {
            return new TestingMesosWorkerStore(
                    stopConsumer,
                    getFrameworkIDSupplier,
                    setFrameworkIDConsumer,
                    recoverWorkersSupplier,
                    newTaskIDSupplier,
                    putWorkerConsumer,
                    removeWorkerFunction);
        }
    }
}
