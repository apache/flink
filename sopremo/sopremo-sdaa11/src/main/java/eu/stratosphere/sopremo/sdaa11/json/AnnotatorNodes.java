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
 * Structure: {"annotation":int; "annotatee":obj}
 * 
 * @author skruse
 *
 */
public class AnnotatorNodes {
	
	public static final String ANNOTATEE = "value";
	public static final String ANNOTATION = "key";

	public static void annotate(ObjectNode node, IntNode annotation, IJsonNode annotatee) {
		node.put(ANNOTATION, annotation);
		node.put(ANNOTATEE, annotatee);
	}
	
	public static IntNode getAnnotation(ObjectNode annotatorNode) {
		return (IntNode) annotatorNode.get(ANNOTATION);
	}
	
	public static IJsonNode getAnnotatee(ObjectNode annotatorNode) {
		return annotatorNode.get(ANNOTATEE);
	}
	

}
