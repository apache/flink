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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * This interface is for a {@link Catalog} to listen on temporary object operations. When a catalog
 * implements this interface, it'll get informed when certain operations are performed on temporary
 * objects belonging to that catalog.
 */
@PublicEvolving
public interface TemporaryOperationListener {

    /**
     * This method is called when a temporary table or view is to be created in this catalog. The
     * catalog can modify the table or view according to its needs and return the modified
     * CatalogBaseTable instance, which will be stored for the user session.
     *
     * @param tablePath path of the table or view to be created
     * @param table the table definition
     * @return the modified table definition to be stored
     * @throws CatalogException in case of any runtime exception
     */
    CatalogBaseTable onCreateTemporaryTable(ObjectPath tablePath, CatalogBaseTable table)
            throws CatalogException;

    /**
     * This method is called when a temporary table or view in this catalog is to be dropped.
     *
     * @param tablePath path of the table or view to be dropped
     * @throws CatalogException in case of any runtime exception
     */
    void onDropTemporaryTable(ObjectPath tablePath) throws CatalogException;

    /**
     * This method is called when a temporary model is to be created in this catalog. The catalog
     * can modify the model according to its needs and return the modified CatalogModel instance,
     * which will be stored for the user session.
     *
     * @param modelPath path of the model to be created
     * @param model the model definition
     * @return the modified model definition to be stored
     * @throws CatalogException in case of any runtime exception
     */
    CatalogModel onCreateTemporaryModel(ObjectPath modelPath, CatalogModel model)
            throws CatalogException;

    /**
     * This method is called when a temporary model in this catalog is to be dropped.
     *
     * @param modelPath path of the model to be dropped
     * @throws CatalogException in case of any runtime exception
     */
    void onDropTemporaryModel(ObjectPath modelPath) throws CatalogException;

    /**
     * This method is called when a temporary function is to be created in this catalog. The catalog
     * can modify the function according to its needs and return the modified CatalogFunction
     * instance, which will be stored for the user session.
     *
     * @param functionPath path of the function to be created
     * @param function the function definition
     * @return the modified function definition to be stored
     * @throws CatalogException in case of any runtime exception
     */
    CatalogFunction onCreateTemporaryFunction(ObjectPath functionPath, CatalogFunction function)
            throws CatalogException;

    /**
     * This method is called when a temporary function in this catalog is to be dropped.
     *
     * @param functionPath path of the function to be dropped
     * @throws CatalogException in case of any runtime exception
     */
    void onDropTemporaryFunction(ObjectPath functionPath) throws CatalogException;
}
