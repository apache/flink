/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.api.java.tuple;

/**
 *
 */
public abstract class Tuple implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final int MAX_ARITY = 22;
	
	
	public abstract <T> T getField(int pos);
	
	public abstract <T> void setField(T value, int pos);
	
	public abstract <T> T getFieldFast(int pos);

	public abstract int getArity();
}
