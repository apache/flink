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

package eu.stratosphere.pact.compiler.properties;


/**
 *
 *
 * 
 */
public abstract class Property<T extends Property<T>>
{
	protected final int[] fields;
	
	
	protected Property(int[] fields)
	{
		if (fields == null)
			throw new NullPointerException();
		
		this.fields = fields;
	}
	
	
	public int[] getFields()
	{
		return this.fields;
	}
	
	public boolean areTheseFieldsPrefix(int[] otherFields)
	{
		if (otherFields == null || otherFields.length < this.fields.length) {
			return false;
		}
		
		for (int i = 0; i < this.fields.length; i++) {
			if (this.fields[i] != otherFields[i])
				return false;
		}
		
		return true;
	}
	
	public boolean areOtherFieldsPrefix(int[] otherFields)
	{
		if (otherFields == null || otherFields.length > this.fields.length) {
			return false;
		}
		
		for (int i = 0; i < otherFields.length; i++) {
			if (this.fields[i] != otherFields[i])
				return false;
		}
		
		return true;
	}
	
	
	public abstract String getName();
	
	public abstract boolean satisfiesRequiredProperty(T requiredProperty);
	
	public abstract boolean isTrivial();
	
}
