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

package org.apache.flink.ml.api.misc.importation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.model.ModelFormat;

/**
 * Base interface of model importers. Importers import model data in the specific source format as a
 * flink-ml Model. The source format is defined in {@link ModelFormat}.
 *
 * <p>Users are recommended not to create an Importer instance with constructors but use
 * {@link ImporterLoader#getImporter}.
 */
@PublicEvolving
public abstract class Importer<M extends Model, V> {

	/**
	 * Imports the model data in source format and returns the result Model.
	 *
	 * @param value model data to import
	 * @return model loaded with the model data
	 */
	public abstract M load(V value);

	/**
	 * Returns the source format of this Importer.
	 *
	 * @return source format of this Importer
	 */
	public abstract ModelFormat getSourceFormat();
}
