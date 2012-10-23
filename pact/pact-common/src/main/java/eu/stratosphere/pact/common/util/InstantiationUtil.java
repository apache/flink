/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;


/**
 * Utility class to create instances from class objects and checking failure reasons.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class InstantiationUtil
{
	/**
	 * Creates a new instance of the given class.
	 * 
	 * @param <T> The generic type of the class.
	 * @param clazz The class to instantiate.
	 * @param castTo Optional parameter, specifying the class that the given class must be a subclass off. This
	 *               argument is added to prevent class cast exceptions occurring later. 
	 * @return An instance of the given class.
	 * 
	 * @throws RuntimeException Thrown, if the class could not be instantiated. The exception contains a detailed
	 *                          message about the reason why the instantiation failed.
	 */
	public static <T> T instantiate(Class<T> clazz, Class<? super T> castTo)
	{
		if (clazz == null) {
			throw new NullPointerException();
		}
		
		// check if the class is a subclass, if the check is required
		if (castTo != null && !castTo.isAssignableFrom(clazz)) {
			throw new RuntimeException("The class '" + clazz.getName() + "' is not a subclass of '" + 
				castTo.getName() + "' as is required.");
		}
		
		// try to instantiate the class
		try {
			return clazz.newInstance();
		}
		catch (InstantiationException iex)
		{
			// check what the cause was
			final String errorMessage;
			if (!isProperClass(clazz)) {
				errorMessage = "The class is no proper class, it is either abstract, an interface, or a primitive type.";
			}
			else if (!hasPublicNullaryConstructor(clazz)) {
				errorMessage = "The class is missing a public nullary constructor (constructor without arguments).";
			}
			else {
				// the error was most likely an exception in the constructor or field initialization
				throw new RuntimeException("Could not instantiate type '" + clazz.getName() + 
					"' due to an unspecified exception: " + iex.getMessage(), iex);
			}
			throw new RuntimeException("Could not instantiate type '" + clazz.getName() + 
				"': " + errorMessage);
		}
		catch (IllegalAccessException iaex) {
			final String errorMessage;
			if (!isPublic(clazz)) {
				errorMessage = "The class is not public.";
			}
			else {
				errorMessage = "The nullary constructor is not public.";
			}
			throw new RuntimeException("Could not instantiate type '" + clazz.getName() + 
				"': " + errorMessage);
		}
		catch (Throwable t) {
			String message = t.getMessage();
			throw new RuntimeException("Could not instantiate type '" + clazz.getName() + 
				"' Most likely the constructor (or a member variable initialization) threw an exception" + 
				(message == null ? "." : ": " + message), t);
		}
	}
	
	/**
	 * Checks, whether the given class has a public nullary constructor.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class has a public nullary constructor, false if not.
	 */
	public static boolean hasPublicNullaryConstructor(Class<?> clazz)
	{
		Constructor<?>[] constructors = clazz.getConstructors();
		for (int i = 0; i < constructors.length; i++) {
			if (constructors[i].getParameterTypes().length == 0) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Checks, whether the given class is public.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class is public, false if not.
	 */
	public static boolean isPublic(Class<?> clazz)
	{
		return Modifier.isPublic(clazz.getModifiers());
	}
	
	/**
	 * Checks, whether the class is a proper class, i.e. not abstract or an interface, and not a primitive type.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class is a proper class, false otherwise.
	 */
	public static boolean isProperClass(Class<?> clazz)
	{
		int mods = clazz.getModifiers();
		return !(Modifier.isAbstract(mods) || Modifier.isInterface(mods) || Modifier.isNative(mods));
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Private constructor to prevent instantiation.
	 */
	private InstantiationUtil()
	{
		throw new RuntimeException();
	}
}
