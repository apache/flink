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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for external resources and external resource drivers.
 */
@PublicEvolving
public class ExternalResourceOptions {

	/** The amount of the external resource per task executor. This is used as a suffix in an actual config. */
	public static final String EXTERNAL_RESOURCE_AMOUNT_SUFFIX = "amount";

	/** The driver factory class of the external resource to use. This is used as a suffix in an actual config. */
	public static final String EXTERNAL_RESOURCE_DRIVER_FACTORY_SUFFIX = "driver-factory.class";

	/** The suffix of custom config options' prefix for the external resource. */
	public static final String EXTERNAL_RESOURCE_DRIVER_PARAM_SUFFIX = "param.";

	/** The naming pattern of custom config options for the external resource. This is used as a suffix. */
	private static final String EXTERNAL_RESOURCE_DRIVER_PARAM_PATTERN_SUFFIX = EXTERNAL_RESOURCE_DRIVER_PARAM_SUFFIX + "<param>";

	/**
	 * The prefix for all external resources configs. Has to be combined with a resource name and
	 * the configs mentioned below.
	 */
	private static final String EXTERNAL_RESOURCE_PREFIX = "external-resource";

	/**
	 * List of the resource_name of all external resources with delimiter ";", e.g. "gpu;fpga" for two external resource gpu and fpga.
	 * The resource_name will be used to splice related config options for external resource. Only the resource_name defined here will
	 * go into effect in external resource framework.
	 *
	 * <p>Example:
	 * <pre>{@code
	 * external-resources = gpu;fpga
	 *
	 * external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory
	 * external-resource.gpu.amount: 2
	 * external-resource.gpu.param.type: nvidia
	 *
	 * external-resource.fpga.driver-factory.class: org.apache.flink.externalresource.fpga.FPGADriverFactory
	 * external-resource.fpga.amount: 1
	 * }</pre>
	 */
	public static final ConfigOption<List<String>> EXTERNAL_RESOURCE_LIST =
		key("external-resources")
			.stringType()
			.asList()
			.defaultValues()
			.withDescription("List of the <resource_name> of all external resources with delimiter \";\", e.g. \"gpu;fpga\" " +
				"for two external resource gpu and fpga. The <resource_name> will be used to splice related config options for " +
				"external resource. Only the <resource_name> defined here will go into effect by external resource framework.");

	/**
	 * Defines the factory class name for the external resource identified by &gt;resource_name&lt;. The factory will be used
	 * to instantiate the {@link org.apache.flink.api.common.externalresource.ExternalResourceDriver} at the TaskExecutor side.
	 *
	 * <p>It is intentionally included into user docs while unused.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> EXTERNAL_RESOURCE_DRIVER_FACTORY_CLASS =
		key(genericKeyWithSuffix(EXTERNAL_RESOURCE_DRIVER_FACTORY_SUFFIX))
			.stringType()
			.noDefaultValue()
			.withDescription("Defines the factory class name for the external resource identified by <resource_name>. The " +
				"factory will be used to instantiated the ExternalResourceDriver at the TaskExecutor side. For example, " +
				"org.apache.flink.externalresource.gpu.GPUDriverFactory");

	/**
	 * The amount for the external resource specified by &gt;resource_name&lt; per TaskExecutor.
	 *
	 * <p>It is intentionally included into user docs while unused.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final ConfigOption<Long> EXTERNAL_RESOURCE_AMOUNT =
		key(genericKeyWithSuffix(EXTERNAL_RESOURCE_AMOUNT_SUFFIX))
			.longType()
			.noDefaultValue()
			.withDescription("The amount for the external resource specified by <resource_name> per TaskExecutor.");

	/**
	 * The naming pattern of custom config options for the external resource specified by &gt;resource_name&lt;.
	 * Only the configurations that follow this pattern would be passed into the driver factory of that external resource.
	 *
	 * <p>It is intentionally included into user docs while unused.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> EXTERNAL_RESOURCE_DRIVER_PARAM =
		key(genericKeyWithSuffix(EXTERNAL_RESOURCE_DRIVER_PARAM_PATTERN_SUFFIX))
			.stringType()
			.noDefaultValue()
			.withDescription("The naming pattern of custom config options for the external resource specified by <resource_name>. " +
				"Only the configurations that follow this pattern would be passed into the driver factory of that external resource.");

	public static String genericKeyWithSuffix(String suffix) {
		return keyWithResourceNameAndSuffix("<resource_name>", suffix);
	}

	/**
	 * Generate the config option key with resource_name and suffix.
	 */
	private static String keyWithResourceNameAndSuffix(String resourceName, String suffix) {
		return String.format("%s.%s.%s", EXTERNAL_RESOURCE_PREFIX, Preconditions.checkNotNull(resourceName), Preconditions.checkNotNull(suffix));
	}

	/**
	 * Generate the config option key for the amount of external resource with resource_name.
	 */
	public static String getAmountConfigOptionForResource(String resourceName) {
		return keyWithResourceNameAndSuffix(resourceName, EXTERNAL_RESOURCE_AMOUNT_SUFFIX);
	}

	/**
	 * Generate the config option key for the configuration key of external resource in the deploying system.
	 */
	public static String getSystemConfigKeyConfigOptionForResource(String resourceName, String suffix) {
		return keyWithResourceNameAndSuffix(resourceName, suffix);
	}

	/**
	 * Generate the config option key for the factory class name of {@link org.apache.flink.api.common.externalresource.ExternalResourceDriver}.
	 */
	public static String getExternalResourceDriverFactoryConfigOptionForResource(String resourceName) {
		return keyWithResourceNameAndSuffix(resourceName, EXTERNAL_RESOURCE_DRIVER_FACTORY_SUFFIX);
	}

	/**
	 * Generate the suffix option key prefix for the user-defined params for external resources.
	 */
	public static String getExternalResourceParamConfigPrefixForResource(String resourceName) {
		return keyWithResourceNameAndSuffix(resourceName, EXTERNAL_RESOURCE_DRIVER_PARAM_SUFFIX);
	}
}
