/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.sdaa11.json;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * Convenience class for annotator nodes. <br>
 * Structure: {"key":int; "value":obj}
 * 
 * @author skruse
 * 
 */
public class AnnotatorNodes {

	public static final String ANNOTATEE = "value";
	public static final String ANNOTATION = "key";
	public static final String FLAT_ANNOTATION = "__key__";

	public static void annotate(final ObjectNode node,
			final IntNode annotation, final IJsonNode annotatee) {
		annotate(node, ANNOTATION, annotation, ANNOTATEE, annotatee);
	}
	
	public static void annotate(final ObjectNode node,
			String annotationKey, final IntNode annotation, 
			String annotateeKey, final IJsonNode annotatee) {
		node.put(annotationKey, annotation);
		node.put(annotateeKey, annotatee);

	}

	public static IntNode getAnnotation(final ObjectNode annotatorNode) {
		return (IntNode) annotatorNode.get(ANNOTATION);
	}

	public static IJsonNode getAnnotatee(final ObjectNode annotatorNode) {
		return annotatorNode.get(ANNOTATEE);
	}

	public static void flatAnnotate(final ObjectNode node,
			final IntNode annotation) {
		node.put(FLAT_ANNOTATION, annotation);
	}

	public static IntNode getFlatAnnotation(final ObjectNode node) {
		return (IntNode) node.get(FLAT_ANNOTATION);
	}

}
