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

package org.apache.flink.mesos;

import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.apache.mesos.Protos;

import java.net.URL;
import java.util.Arrays;

import scala.Option;

/**
 * Collection of utility methods.
 */
public class Utils {
	/**
	 * Construct a Mesos environment variable.
	 */
	public static Protos.Environment.Variable variable(String name, String value) {
		return Protos.Environment.Variable.newBuilder()
			.setName(name)
			.setValue(value)
			.build();
	}

	/**
	 * Construct a Mesos URI.
	 */
	public static Protos.CommandInfo.URI uri(URL url, boolean cacheable) {
		return Protos.CommandInfo.URI.newBuilder()
			.setValue(url.toExternalForm())
			.setExtract(false)
			.setCache(cacheable)
			.build();
	}

	/**
	 * Construct a Mesos URI.
	 */
	public static Protos.CommandInfo.URI uri(MesosArtifactResolver resolver, ContainerSpecification.Artifact artifact) {
		Option<URL> url = resolver.resolve(artifact.dest);
		if (url.isEmpty()) {
			throw new IllegalArgumentException("Unresolvable artifact: " + artifact.dest);
		}

		return Protos.CommandInfo.URI.newBuilder()
			.setValue(url.get().toExternalForm())
			.setOutputFile(artifact.dest.toString())
			.setExtract(artifact.extract)
			.setCache(artifact.cachable)
			.setExecutable(artifact.executable)
			.build();
	}

	/**
	 * Construct a scalar resource value.
	 */
	public static Protos.Resource scalar(String name, double value) {
		return Protos.Resource.newBuilder()
			.setName(name)
			.setType(Protos.Value.Type.SCALAR)
			.setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
			.build();
	}

	/**
	 * Construct a range value.
	 */
	public static Protos.Value.Range range(long begin, long end) {
		return Protos.Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
	}

	/**
	 * Construct a ranges resource value.
	 */
	public static Protos.Resource ranges(String name, Protos.Value.Range... ranges) {
		return Protos.Resource.newBuilder()
			.setName(name)
			.setType(Protos.Value.Type.RANGES)
			.setRanges(Protos.Value.Ranges.newBuilder().addAllRange(Arrays.asList(ranges)).build())
			.build();
	}
}
