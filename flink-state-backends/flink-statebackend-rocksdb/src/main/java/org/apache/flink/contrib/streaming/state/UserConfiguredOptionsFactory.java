/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

/**
 * This is a encapsulation of user defined {@link OptionsFactory} through flink-conf.yaml.
 * Keep in mind that before creating DBOptions and ColumnFamilyOptions,
 * {@link #instantiateOptionsFactory(ClassLoader)} must be first called with given classloader.
 */
public final class UserConfiguredOptionsFactory implements OptionsFactory{

	private final String className;
	private OptionsFactory optionsFactory;

	UserConfiguredOptionsFactory(String className) {
		this.className = className;
	}

	/**
	 * Instantiate the pre-configured options factory with given class loader.
	 *
	 * @param classLoader The class loader that should be used to load the options factory.
	 * @throws DynamicCodeLoadingException Thrown if a options factory is configured and the factory class was not
	 *             found or the factory could not be instantiated.
	 */
	public void instantiateOptionsFactory(ClassLoader classLoader) throws DynamicCodeLoadingException {
		if (optionsFactory == null) {
			try {
				@SuppressWarnings("rawtypes")
				Class<? extends OptionsFactory> clazz =
					Class.forName(className, false, classLoader)
						.asSubclass(OptionsFactory.class);

				this.optionsFactory = clazz.newInstance();
			} catch (ClassNotFoundException e) {
				throw new DynamicCodeLoadingException(
					"Cannot find configured options factory class: " + className, e);
			} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
				throw new DynamicCodeLoadingException("The class configured under '" +
					RocksDBOptions.OPTIONS_FACTORY.key() + "' is not a valid options factory (" +
					className + ')', e);
			}
		}
	}

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions) {
		Preconditions.checkNotNull(optionsFactory,
			"user-configured OptionsFactory has not been initialized, please ensure 'instantiateOptionsFactory' has been called.");
		return optionsFactory.createDBOptions(currentOptions);
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
		Preconditions.checkNotNull(optionsFactory,
			"user-configured OptionsFactory has not been initialized, please ensure 'instantiateOptionsFactory' has been called.");
		return optionsFactory.createColumnOptions(currentOptions);
	}

	public String getClassName() {
		return className;
	}
}
