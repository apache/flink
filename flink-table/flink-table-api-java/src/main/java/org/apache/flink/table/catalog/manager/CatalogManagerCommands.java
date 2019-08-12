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

package org.apache.flink.table.catalog.manager;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.Optional;

/**
 * Commands that can be executed on {@link CatalogManager}. They unify the access to a catalog and exception handling
 * related to that.
 */
@Internal
public final class CatalogManagerCommands {

	/**
	 * Creates a command that will create a table in a given fully qualified path.
	 *
	 * @param table table to put in the given path
	 * @param ignoreIfExists if true exception will be thrown if a table exists in the given path
	 * @return pre configured catalog command
	 */
	public static ModifyCatalog createTable(CatalogBaseTable table, boolean ignoreIfExists) {
		return new CreateTable(table, ignoreIfExists);
	}

	/**
	 * Creates a command that will alter a table in a given fully qualified path.
	 *
	 * @param table table to put in the given path
	 * @param ignoreIfNotExists if true exception will be thrown if the table to be altered does not exist in
	 * the catalog
	 * @return pre configured catalog command
	 */
	public static ModifyCatalog alterTable(CatalogBaseTable table, boolean ignoreIfNotExists) {
		return new AlterTable(table, ignoreIfNotExists);
	}

	/**
	 * Creates a command that will drop a table in a given fully qualified path.
	 *
	 * @param ignoreIfNotExists if true exception will be thrown if the table to be dropped does not exist in
	 * the catalog
	 * @return pre configured catalog command
	 */
	public static ModifyCatalog dropTable(boolean ignoreIfNotExists) {
		return new DropTable(ignoreIfNotExists);
	}

	/**
	 * A command that modifies given {@link CatalogManager} in a {@link ObjectIdentifier}. This unifies error handling
	 * across different commands.
	 */
	public interface ModifyCatalog {
		void executeIn(CatalogManager catalog, ObjectIdentifier identifier);
	}

	private abstract static class AbstractModifyCatalog implements ModifyCatalog {
		@Override
		public final void executeIn(CatalogManager manager, ObjectIdentifier identifier) {
			Optional<Catalog> catalog = manager.getCatalog(identifier.getCatalogName());
			if (catalog.isPresent()) {
				try {
					execute(catalog.get(), identifier.toObjectPath());
				} catch (Exception e) {
					throw new TableException(
						String.format("Could not execute %s in path %s", toString(), identifier),
						e);
				}
			} else {
				handleNoCatalog(identifier);
			}
		}

		protected abstract void execute(Catalog catalog, ObjectPath path) throws Exception;

		protected void handleNoCatalog(ObjectIdentifier identifier) {
			throw new TableException(String.format("Catalog %s does not exist.", identifier.getCatalogName()));
		}
	}

	private static final class DropTable extends AbstractModifyCatalog {
		private final boolean ignoreIfNotExists;

		private DropTable(boolean ignoreIfNotExists) {
			this.ignoreIfNotExists = ignoreIfNotExists;
		}

		@Override
		protected void execute(Catalog catalog, ObjectPath path) throws Exception {
			catalog.dropTable(path, ignoreIfNotExists);
		}

		@Override
		protected void handleNoCatalog(ObjectIdentifier identifier) {
			if (!ignoreIfNotExists) {
				super.handleNoCatalog(identifier);
			}
		}

		@Override
		public String toString() {
			return "DropTable";
		}
	}

	private static final class AlterTable extends AbstractModifyCatalog {
		private final CatalogBaseTable table;
		private final boolean ignoreIfExists;

		private AlterTable(CatalogBaseTable table, boolean ignoreIfNotExists) {
			this.table = table;
			this.ignoreIfExists = ignoreIfNotExists;
		}

		@Override
		protected void execute(Catalog catalog, ObjectPath path) throws Exception {
			catalog.alterTable(path, table, ignoreIfExists);
		}

		@Override
		public String toString() {
			return "AlterTable";
		}
	}

	private static final class CreateTable extends AbstractModifyCatalog {
		private final CatalogBaseTable table;
		private final boolean ignoreIfExists;

		private CreateTable(CatalogBaseTable table, boolean ignoreIfExists) {
			this.table = table;
			this.ignoreIfExists = ignoreIfExists;
		}

		@Override
		protected void execute(Catalog catalog, ObjectPath path) throws Exception {
			catalog.createTable(path, table, ignoreIfExists);
		}

		@Override
		public String toString() {
			return "CreateTable";
		}
	}

	private CatalogManagerCommands() {
	}
}
