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

package org.apache.flink.ml.api.misc.exportation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.model.ModelFormat;

import java.util.List;

/**
 * Base interface of model exporters. Exporters export models to the specific target format which is
 * defined in {@link ModelFormat}.
 *
 * <p>Users are recommended not to create an Exporter instance with constructors but use
 * {@link ExporterLoader#getExporter}.
 */
@PublicEvolving
public abstract class Exporter<M extends Model, V> {

	/**
	 * Exports the model to model data in target format.
	 *
	 * @param model model to export
	 * @return model data in target format
	 */
	public abstract V export(M model);

	/**
	 * Returns the target format of this Exporter.
	 *
	 * @return target format of this Exporter
	 */
	public abstract ModelFormat getTargetFormat();

	/**
	 * Returns a list of model class names that this Exporter supports to export. The values in the
	 * list should be in the same format returned by {@link java.lang.Class#getName}.
	 *
	 * @return a list of model class names that this Exporter supports to export
	 */
	public abstract List<String> supportedModelClassNames();
}
