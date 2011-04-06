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

package eu.stratosphere.pact.runtime.io;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author nijkamp
 *
 */
public class DeepCopyTest 
{
	public final static class Nested
	{
		private Integer integer;
		
		public Nested()
		{
			
		}
		
		public Nested(int integer)
		{
			this.integer = integer;
		}
		
		public int getInt()
		{
			return integer;
		}
	}
	
	public final static class ToClone
	{
		private List<Nested> list = new ArrayList<Nested>();
		
		public ToClone()
		{
			
		}
		
		public ToClone(Integer ... ints)
		{
			for(int integer : ints)
			{
				list.add(new Nested(integer));
			}
		}
		
		public int get(int pos)
		{
			return list.get(pos).getInt();
		}
	}
	
	public static void main(String ... args) throws Exception
	{
		ToClone toClone = new ToClone(1,2,3,4,5);
		{
			final int count = 10000;
			for(int i = 0; i < count; i++)
			{
				clone(toClone);
			}
			// System.out.println("clone: " + count + " objects -> " + (System.currentTimeMillis()-start) + "ms");
		}
	}
	
	
	@SuppressWarnings("unchecked")
	private static <T> T clone(T toClone) throws Exception
	{
		if(toClone == null)
		{
			return null;
		}
		else if (String.class.equals(toClone.getClass())) 
		{
			return (T) new java.lang.String((java.lang.String) toClone); 
		}
		else if (Integer.class.equals(toClone.getClass())) 
		{ 
			return (T) new java.lang.Integer((java.lang.Integer) toClone); 
		}
		else if (toClone.getClass().isArray()) 
		{ 
			final int length = java.lang.reflect.Array.getLength(toClone); 
			T clone = (T) java.lang.reflect.Array.newInstance(toClone.getClass().getComponentType(), length);
			for (int i = 0; i < length; i++) 
			{
				Object element = java.lang.reflect.Array.get(toClone, i);
				Object copy = clone(element);
				java.lang.reflect.Array.set(clone, i, copy);
			}
			return clone;
		}
		else
		{
			T clone = (T) toClone.getClass().newInstance();
			cloneMembers(toClone, clone);
			return clone;
		}
	}
	
	private static void cloneMembers(Object toCopy, Object copy) throws Exception
	{
		for(Field field : toCopy.getClass().getDeclaredFields())
		{
		    if (!Modifier.isPublic(field.getModifiers()) ||
		            !Modifier.isPublic(field.getDeclaringClass().getModifiers()))
		    {
		          field.setAccessible(true);
		    }
		    
		    if(Modifier.isStatic(field.getModifiers()))
		    {
		    	continue;
		    }
			
			// System.out.println("field=" + field.getName());
			
			if(field.getType().isPrimitive())
			{
				field.set(copy, field.get(toCopy));
			}
			else
			{
				field.set(copy, clone(field.get(toCopy)));
			}
		}
	}
}
