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

package org.apache.flink.modelserving.java.model;

import org.apache.flink.model.Modeldescriptor;

import java.util.Arrays;
import java.util.Optional;

/**
 * Data converter - collection of static methods for data transformation.
 */
public class DataConverter {

	// Model factories converter
	private static ModelFacroriesResolver<?, ?> resolver = null;

	/**
	 * Default data converter constructor (private).
	 *
	 */
	private DataConverter(){}

	/**
	 * Setting Model factories converter. Has to be invoked by the user at the beginning of his code.
	 *
	 * @param res model factories resolver.
	 */
	public static <RECORD, RESULT> void setResolver(ModelFacroriesResolver<RECORD, RESULT> res){
		resolver = res;
	}

	/**
	 * Validating that resolver is set.
	 * @return  boolean, specifying whether resolver is set
	 */
	private static boolean validateResolver() {
		if (resolver == null){
			System.out.println("Model factories resolver is not set");
			return false;
		}
		return true;
	}

	/**
 	 * Convert byte array to ModelToServe.
 	 *
 	 * @param binary byte representation of ModelDescriptor.proto.
	 * @return model to serve.
 	*/
	public static Optional<ModelToServe> convertModel(byte[] binary){
		try {
			// Unmarshall record
			Modeldescriptor.ModelDescriptor model = Modeldescriptor.ModelDescriptor.parseFrom(binary);
			// Return it
			if (model.getMessageContentCase().equals(Modeldescriptor.ModelDescriptor.MessageContentCase.DATA)){
				return Optional.of(new ModelToServe(
					model.getName(), model.getDescription(), model.getModeltype(),
					model.getData().toByteArray(), null, model.getDataType()));
			}
			else {
				return Optional.of(new ModelToServe(
					model.getName(), model.getDescription(), model.getModeltype(),
					new byte[0], model.getLocation(), model.getDataType()));
			}
		} catch (Throwable t) {
			// Oops
			System.out.println("Exception parsing input record" + new String(binary));
			t.printStackTrace();
			return Optional.empty();
		}
	}

	/**
	 * Create Model's deep copy.
	 *
	 * @param model model.
	 * @return model's copy.
	 */
	// Deep copy of model
	public static <RECORD, RESULT> Model<RECORD, RESULT> copy(Model<RECORD, RESULT> model) {
		if (!validateResolver()) {
			return null;
		}
		if (model == null) {
			return null;
		}
		else {
			ModelFactory<RECORD, RESULT> factory =
				(ModelFactory<RECORD, RESULT>) resolver.getFactory((int) model.getType());
			if (factory != null) {
				byte[] bytes = model.getBytes();
				return factory.restore(Arrays.copyOf(bytes, bytes.length));
			}
			return null;
		}
	}

	/**
	 * Restore model from byte array.
	 *
	 * @param t model's type.
	 * @param content model's content (byte array).
	 * @return model.
	 */
	public static <RECORD, RESULT> Model<RECORD, RESULT> restore(int t, byte[] content){
		if (!validateResolver()) {
			return null;
		}
		ModelFactory<RECORD, RESULT> factory = (ModelFactory<RECORD, RESULT>) resolver.getFactory(t);
		if (factory != null) {
			return factory.restore(content);
		}
		return null;
	}

	/**
	 * Convert ModelToServe to Model.
	 *
	 * @param model ModelToServe.
	 * @return model.
	 */
	public static <RECORD, RESULT> Optional<Model<RECORD, RESULT>> toModel(ModelToServe model){
		if (!validateResolver()) {
			return Optional.empty();
		}
		ModelFactory<RECORD, RESULT> factory =
			(ModelFactory<RECORD, RESULT>) resolver.getFactory(model.getModelType().getNumber());
		if (factory != null) {
			return factory.create(model);
		}
		System.out.println("Did not find Model factory for type " + model.getModelType() +
			" with number " + model.getModelType().getNumber());
		return Optional.empty();
	}
}
