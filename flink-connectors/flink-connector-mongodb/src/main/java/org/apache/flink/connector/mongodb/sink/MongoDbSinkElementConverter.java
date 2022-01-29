/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

/**
 * An implementation of the {@link ElementConverter} that uses the MongoDb api. The user only needs
 * to provide a {@link DocumentGenerator} of the {@code InputT} and a {@link FilterGenerator} lambda
 * to transform the input element into a MongoDb Filter expression.
 */
@Internal
public class MongoDbSinkElementConverter<InputT>
        implements ElementConverter<InputT, MongoReplaceOneModel> {

    /**
     * A Document Generator to specify how the input element should be converted to a MongoDb
     * document.
     */
    private final DocumentGenerator<InputT> documentGenerator;

    /**
     * A Filter generator functional interface that produces a Filter Document from the input
     * element.
     */
    private final FilterGenerator<InputT> filterGenerator;

    private MongoDbSinkElementConverter(
            DocumentGenerator<InputT> documentGenerator, FilterGenerator<InputT> filterGenerator) {
        this.documentGenerator = documentGenerator;
        this.filterGenerator = filterGenerator;
    }

    @Override
    public MongoReplaceOneModel apply(InputT element, SinkWriter.Context context) {
        return new MongoReplaceOneModel(
                filterGenerator.apply(element), documentGenerator.apply(element));
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the MongoDbSinkElementConverter. */
    public static class Builder<InputT> {

        private DocumentGenerator<InputT> documentGenerator;
        private FilterGenerator<InputT> filterGenerator;

        public Builder<InputT> setDocumentGenerator(DocumentGenerator<InputT> documentGenerator) {
            this.documentGenerator = documentGenerator;
            return this;
        }

        public Builder<InputT> setFilterGenerator(FilterGenerator<InputT> filterGenerator) {
            this.filterGenerator = filterGenerator;
            return this;
        }

        public MongoDbSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    documentGenerator,
                    "No DocumentGenerator was supplied to the "
                            + "MongoDbSinkElementConverter builder.");
            Preconditions.checkNotNull(
                    filterGenerator,
                    "No FilterGenerator lambda was supplied to the "
                            + "MongoDbSinkElementConverter builder.");
            return new MongoDbSinkElementConverter<>(documentGenerator, filterGenerator);
        }
    }
}
