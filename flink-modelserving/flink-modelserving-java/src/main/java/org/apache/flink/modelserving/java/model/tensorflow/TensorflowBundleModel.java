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

package org.apache.flink.modelserving.java.model.tensorflow;

import org.apache.flink.annotation.Public;
import org.apache.flink.model.Modeldescriptor;
import org.apache.flink.modelserving.java.model.Model;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.tensorflow.Graph;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.framework.MetaGraphDef;
import org.tensorflow.framework.SavedModel;
import org.tensorflow.framework.SignatureDef;
import org.tensorflow.framework.TensorInfo;
import org.tensorflow.framework.TensorShapeProto;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Base class for tensorflow (bundled) model processing.
 * This is experimental implementation showing how to deal with bundle. The implementation leverages
 * local file system access in both constructor and get tag method. A real implementation will use
 * some kind of shared storage, for example S3, Minio, GKS, etc.
 */
@Public
public abstract class TensorflowBundleModel<RECORD, RESULT> implements Model<RECORD, RESULT> {

	// Tensorflow graph
	protected Graph graph;
	// Tensorflow session
	protected Session session;
	// Signatures
	protected  Map<String, TSignature> signatures;
	// Tags
	protected List<String> tags;
	// Path;
	private String path;

	/**
	 * Creates a new tensorflow (optimized) model.
	 *
	 * @param input binary representation of tensorflow(optimized) model.
	 */
	public TensorflowBundleModel(byte[] input) throws Throwable {

		// Convert input into file path
		path = new String(input);
		// get tags. We assume here that the first tag is the one we use
		tags = getTags(path);
		if (tags.size() > 0) {
			// get saved model bundle
			SavedModelBundle bundle = SavedModelBundle.load(path, tags.get(0));
			// get grapth
			graph = bundle.graph();
			// get metatagraph and signature
			MetaGraphDef metaGraphDef = null;
			try {
				metaGraphDef = MetaGraphDef.parseFrom(bundle.metaGraphDef());
				Map<String, SignatureDef> signatureMap = metaGraphDef.getSignatureDefMap();
				//  parse signature, so that we can use definitions (if necessary) programmatically in score method

				signatures = parseSignatures(signatureMap);
			} catch (Throwable e) {
				System.out.println("Error parcing metagraph for " + path);
				signatures = null;
			}
			// Create tensorflow session
			session = bundle.session();
		}
		else {
			throw new Exception("Can't get Tensorflow bundle model");
		}
	}

	/**
	 * Clean up tensorflow model.
	 */
	@Override
	public void cleanup() {
		if (session != null) {
			session.close();
		}
		if (graph != null) {
			graph.close();
		}
	}

	/**
	 * Get bytes representation of tensorflow model.
	 * @return binary representation of the tensorflow model.
	 */
	@Override
	public byte[] getBytes() {
		return path.getBytes();
	}

	/**
	 * Get model'a type.
	 * @return tensorflow (bundled) model type.
	 */
	@Override
	public long getType() {
		return (long) Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED_VALUE;
	}

	/**
	 * Compare 2 tensorflow (bundled) models. They are equal if their binary content is same.
	 * @param obj other model.
	 * @return boolean specifying whether models are the same.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TensorflowBundleModel) {
			return ((TensorflowBundleModel) obj).path.equals(path);
		}
		return false;
	}

	/**
	 * Parse protobuf definition of signature map.
	 * @param signaturedefs map of protobuf encoded signatures.
	 * @return map of signatures.
	 */
	private Map<String, TSignature> parseSignatures(Map<String, SignatureDef> signaturedefs) {

		Map<String, TSignature> signatures = new HashMap<>();
		for (Map.Entry<String, SignatureDef> entry : signaturedefs.entrySet()){
			Map<String, TField> inputs = parseInputOutput(entry.getValue().getInputsMap());
			Map<String, TField> outputs = parseInputOutput(entry.getValue().getOutputsMap());
			signatures.put(entry.getKey(), new TSignature(inputs, outputs));
		}
		return signatures;
	}

	/**
	 * Parse protobuf definition of field map.
	 * @param inputOutputs map of protobuf encoded fields.
	 * @return map of fields.
	 */
	private Map<String, TField> parseInputOutput(Map<String, TensorInfo> inputOutputs){

		Map<String, TField> fields = new HashMap<>();
		for (Map.Entry<String, TensorInfo> entry : inputOutputs.entrySet()){
			String name = "";
			Descriptors.EnumValueDescriptor type = null;
			List<Integer> shape = new LinkedList<>();
			Map<Descriptors.FieldDescriptor, Object> cf = entry.getValue().getAllFields();
			for (Map.Entry<Descriptors.FieldDescriptor, Object> fieldentry : cf.entrySet()){
				if (fieldentry.getKey().getName().contains("shape")){
					List<TensorShapeProto.Dim> dimensions = ((TensorShapeProto) fieldentry.getValue()).getDimList();
					for (TensorShapeProto.Dim d : dimensions) {
						shape.add((int) d.getSize());
					}
					continue;
				}
				if (fieldentry.getKey().getName().contains("name")){
					name = fieldentry.getValue().toString().split(":")[0];
					continue;
				}
				if (fieldentry.getKey().getName().contains("dtype")){
					type = (Descriptors.EnumValueDescriptor) fieldentry.getValue();
					continue;
				}
			}
			fields.put(entry.getKey(), new TField(name, type, shape));
		}
		return fields;
	}

	/**
	 * Get model's tags.
	 * Get tags method. If you want a known tag overwrite this method to return a list (of one) with the required tag.
	 * @param directory Directory where definition is located.
	 * @return map of fields.
	 */
	protected List<String> getTags(String directory) {
		File d = new File(directory);
		List<String> tags = new LinkedList<>();
		List<File> pbfiles = new LinkedList<>();
		if (d.exists() && d.isDirectory()){
			File[] contained = d.listFiles();
			for (File file : contained) {
				if (file.getName().endsWith("pb") || file.getName().endsWith("pbtxt")) {
					pbfiles.add(file);
				}
			}
		}
		try {
			if (pbfiles.size() > 0) {
				byte[] data = Files.readAllBytes(pbfiles.get(0).toPath());
				List<MetaGraphDef> graphs = SavedModel.parseFrom(data).getMetaGraphsList();
				for (MetaGraphDef graph : graphs) {
					List<ByteString> bstrings = graph.getMetaInfoDef().getTagsList().asByteStringList();
					for (ByteString bs : bstrings) {
						tags.add(bs.toStringUtf8());
					}
				}
			}
		}
		catch (Throwable e){
			System.out.println("Error getting tags " + e);
		}
		return tags;
	}
}
