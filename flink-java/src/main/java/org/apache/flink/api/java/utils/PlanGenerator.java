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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.OperatorTranslation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.util.Visitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generator that generates a {@link Plan} from a graph of {@link Operator}s.
 */
public class PlanGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(PlanGenerator.class);

	private final List<DataSink<?>> sinks;
	private final ExecutionConfig config;
	private final List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile;
	private final String jobName;

	public PlanGenerator(
			List<DataSink<?>> sinks,
			ExecutionConfig config,
			List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile,
			String jobName) {
		this.sinks = checkNotNull(sinks);
		this.config = checkNotNull(config);
		this.cacheFile = checkNotNull(cacheFile);
		this.jobName = checkNotNull(jobName);
	}

	public Plan generate() {
		Plan plan = createPlan();

		checkAndRegisterGenericTypeInfo(plan);

		try {
			registerCachedFilesWithPlan(plan);
		} catch (Exception e) {
			throw new RuntimeException("Error while registering cached files: " + e.getMessage(), e);
		}

		printLog();

		return plan;
	}

	/**
	 * Create plan.
	 *
	 * @return the generated plan.
	 */
	private Plan createPlan() {
		OperatorTranslation translator = new OperatorTranslation();
		Plan plan = translator.translateToPlan(sinks, jobName);

		if (config.getParallelism() > 0) {
			plan.setDefaultParallelism(config.getParallelism());
		}
		plan.setExecutionConfig(config);
		return plan;
	}

	/**
	 * Check plan for GenericTypeInfo's and register the types at the serializers.
	 *
	 * @param plan the generated plan.
	 */
	private void checkAndRegisterGenericTypeInfo(Plan plan) {
		if (!config.isAutoTypeRegistrationDisabled()) {
			plan.accept(new Visitor<Operator<?>>() {

				private final Set<Class<?>> registeredTypes = new HashSet<>();
				private final Set<org.apache.flink.api.common.operators.Operator<?>> visitedOperators = new HashSet<>();

				@Override
				public boolean preVisit(org.apache.flink.api.common.operators.Operator<?> visitable) {
					if (!visitedOperators.add(visitable)) {
						return false;
					}
					OperatorInformation<?> opInfo = visitable.getOperatorInfo();
					Serializers.recursivelyRegisterType(opInfo.getOutputType(), config, registeredTypes);
					return true;
				}

				@Override
				public void postVisit(org.apache.flink.api.common.operators.Operator<?> visitable) {
				}
			});
		}
	}

	/**
	 * Registers all files that were registered at this execution environment's cache registry of the
	 * given plan's cache registry.
	 *
	 * @param p The plan to register files at.
	 * @throws IOException Thrown if checks for existence and sanity fail.
	 */
	private void registerCachedFilesWithPlan(Plan p) throws IOException {
		for (Tuple2<String, DistributedCache.DistributedCacheEntry> entry : cacheFile) {
			p.registerCachedFile(entry.f0, entry.f1);
		}
	}

	/**
	 * All types are registered now. Print information.
	 */
	private void printLog() {
		int registeredTypes = getNumberOfRegisteredTypes();
		int defaultKryoSerializers = getNumberOfDefaultKryoSerializers();
		LOG.info("The job has {} registered types and {} default Kryo serializers", registeredTypes,
				defaultKryoSerializers);

		if (config.isForceKryoEnabled() && config.isForceAvroEnabled()) {
			LOG.warn(
					"In the ExecutionConfig, both Avro and Kryo are enforced. Using Kryo serializer for serializing POJOs");
		} else if (config.isForceKryoEnabled()) {
			LOG.info("Using KryoSerializer for serializing POJOs");
		} else if (config.isForceAvroEnabled()) {
			LOG.info("Using AvroSerializer for serializing POJOs");
		}

		logDebuggingTypeDetails();

		// print information about static code analysis
		LOG.debug("Static code analysis mode: {}", config.getCodeAnalysisMode());
	}

	private int getNumberOfRegisteredTypes() {
		return config.getRegisteredKryoTypes().size() +
				config.getRegisteredPojoTypes().size() +
				config.getRegisteredTypesWithKryoSerializerClasses().size() +
				config.getRegisteredTypesWithKryoSerializers().size();
	}

	private int getNumberOfDefaultKryoSerializers() {
		return config.getDefaultKryoSerializers().size() +
				config.getDefaultKryoSerializerClasses().size();
	}

	private void logDebuggingTypeDetails() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Registered Kryo types: {}", config.getRegisteredKryoTypes().toString());
			LOG.debug("Registered Kryo with Serializers types: {}",
					config.getRegisteredTypesWithKryoSerializers().entrySet().toString());
			LOG.debug("Registered Kryo with Serializer Classes types: {}",
					config.getRegisteredTypesWithKryoSerializerClasses().entrySet().toString());
			LOG.debug("Registered Kryo default Serializers: {}",
					config.getDefaultKryoSerializers().entrySet().toString());
			LOG.debug("Registered Kryo default Serializers Classes {}",
					config.getDefaultKryoSerializerClasses().entrySet().toString());
			LOG.debug("Registered POJO types: {}", config.getRegisteredPojoTypes().toString());
		}
	}
}
