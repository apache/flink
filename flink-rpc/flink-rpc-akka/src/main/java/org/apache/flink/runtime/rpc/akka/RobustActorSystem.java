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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FatalExitExceptionHandler;

import akka.actor.ActorSystemImpl;
import akka.actor.BootstrapSetup;
import akka.actor.setup.ActorSystemSetup;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Optional;

import scala.Option;
import scala.concurrent.ExecutionContext;

/**
 * {@link ActorSystemImpl}} which has a configurable {@link
 * java.lang.Thread.UncaughtExceptionHandler}.
 *
 * <p>The class is abstract because instances need to override {@link
 * ActorSystemImpl#uncaughtExceptionHandler()}, as this method is called from the super constructor.
 */
public abstract class RobustActorSystem extends ActorSystemImpl {

    public RobustActorSystem(
            String name,
            Config applicationConfig,
            ClassLoader classLoader,
            Option<ExecutionContext> defaultExecutionContext,
            ActorSystemSetup setup) {
        super(name, applicationConfig, classLoader, defaultExecutionContext, Option.empty(), setup);
    }

    public static RobustActorSystem create(String name, Config applicationConfig) {
        return create(name, applicationConfig, FatalExitExceptionHandler.INSTANCE);
    }

    @VisibleForTesting
    static RobustActorSystem create(
            String name,
            Config applicationConfig,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        return create(
                name,
                ActorSystemSetup.create(
                        BootstrapSetup.create(
                                Optional.empty(),
                                Optional.of(applicationConfig),
                                Optional.empty())),
                Option.apply(uncaughtExceptionHandler));
    }

    private static RobustActorSystem create(
            String name,
            ActorSystemSetup setup,
            Option<Thread.UncaughtExceptionHandler> uncaughtExceptionHandler) {
        final Optional<BootstrapSetup> bootstrapSettings = setup.get(BootstrapSetup.class);
        final ClassLoader classLoader = RobustActorSystem.class.getClassLoader();
        final Config appConfig =
                bootstrapSettings
                        .map(BootstrapSetup::config)
                        .flatMap(RobustActorSystem::toJavaOptional)
                        .orElseGet(() -> ConfigFactory.load(classLoader));
        final Option<ExecutionContext> defaultEC =
                toScalaOption(
                        bootstrapSettings
                                .map(BootstrapSetup::defaultExecutionContext)
                                .flatMap(RobustActorSystem::toJavaOptional));

        final RobustActorSystem robustActorSystem =
                new RobustActorSystem(name, appConfig, classLoader, defaultEC, setup) {
                    @Override
                    public Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
                        return uncaughtExceptionHandler.getOrElse(super::uncaughtExceptionHandler);
                    }
                };
        robustActorSystem.start();
        return robustActorSystem;
    }

    private static <T> Optional<T> toJavaOptional(Option<T> option) {
        return Optional.ofNullable(option.getOrElse(() -> null));
    }

    private static <T> Option<T> toScalaOption(Optional<T> option) {
        return option.map(Option::apply).orElse(Option.empty());
    }
}
