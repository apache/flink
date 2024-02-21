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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Simple container for the request to a handler, that contains the {@link RequestBody} and
 * path/query parameters.
 *
 * @param <R> type of the contained request body
 */
public class HandlerRequest<R extends RequestBody> {

    private final R requestBody;
    private final Collection<File> uploadedFiles;
    private final Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>>
            pathParameters;
    private final Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>>
            queryParameters;

    private HandlerRequest(
            R requestBody,
            Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>>
                    receivedPathParameters,
            Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>>
                    receivedQueryParameters,
            Collection<File> uploadedFiles) {
        this.requestBody = Preconditions.checkNotNull(requestBody);
        this.uploadedFiles =
                Collections.unmodifiableCollection(Preconditions.checkNotNull(uploadedFiles));

        pathParameters = Preconditions.checkNotNull(receivedPathParameters);
        queryParameters = Preconditions.checkNotNull(receivedQueryParameters);
    }

    /**
     * Returns the request body.
     *
     * @return request body
     */
    public R getRequestBody() {
        return requestBody;
    }

    /**
     * Returns the value of the {@link MessagePathParameter} for the given class.
     *
     * @param parameterClass class of the parameter
     * @param <X> the value type that the parameter contains
     * @param <PP> type of the path parameter
     * @return path parameter value for the given class
     * @throws IllegalStateException if no value is defined for the given parameter class
     */
    public <X, PP extends MessagePathParameter<X>> X getPathParameter(Class<PP> parameterClass) {
        @SuppressWarnings("unchecked")
        PP pathParameter = (PP) pathParameters.get(parameterClass);
        Preconditions.checkState(
                pathParameter != null, "No parameter could be found for the given class.");
        return pathParameter.getValue();
    }

    /**
     * Returns the value of the {@link MessageQueryParameter} for the given class.
     *
     * @param parameterClass class of the parameter
     * @param <X> the value type that the parameter contains
     * @param <QP> type of the query parameter
     * @return query parameter value for the given class, or an empty list if no parameter value
     *     exists for the given class
     */
    public <X, QP extends MessageQueryParameter<X>> List<X> getQueryParameter(
            Class<QP> parameterClass) {
        @SuppressWarnings("unchecked")
        QP queryParameter = (QP) queryParameters.get(parameterClass);
        if (queryParameter == null) {
            return Collections.emptyList();
        } else {
            return queryParameter.getValue();
        }
    }

    @Nonnull
    public Collection<File> getUploadedFiles() {
        return uploadedFiles;
    }

    /**
     * Short-cut for {@link #create(RequestBody, MessageParameters, Collection)} without any
     * uploaded files.
     */
    @VisibleForTesting
    public static <R extends RequestBody, M extends MessageParameters> HandlerRequest<R> create(
            R requestBody, M messageParameters) {
        return create(requestBody, messageParameters, Collections.emptyList());
    }

    /**
     * Creates a new {@link HandlerRequest}. The given {@link MessageParameters} are expected to be
     * resolved.
     */
    @VisibleForTesting
    public static <R extends RequestBody, M extends MessageParameters> HandlerRequest<R> create(
            R requestBody, M messageParameters, Collection<File> uploadedFiles) {
        return new HandlerRequest<R>(
                requestBody,
                mapParameters(messageParameters.getPathParameters()),
                mapParameters(messageParameters.getQueryParameters()),
                uploadedFiles);
    }

    /**
     * Creates a new {@link HandlerRequest} after resolving the given {@link MessageParameters}
     * against the given query/path parameter maps.
     *
     * <p>For tests it is recommended to resolve the parameters manually and use {@link #create}.
     */
    public static <R extends RequestBody, M extends MessageParameters>
            HandlerRequest<R> resolveParametersAndCreate(
                    R requestBody,
                    M messageParameters,
                    Map<String, String> receivedPathParameters,
                    Map<String, List<String>> receivedQueryParameters,
                    Collection<File> uploadedFiles)
                    throws HandlerRequestException {

        resolvePathParameters(messageParameters, receivedPathParameters);
        resolveQueryParameters(messageParameters, receivedQueryParameters);

        return create(requestBody, messageParameters, uploadedFiles);
    }

    private static void resolvePathParameters(
            MessageParameters messageParameters, Map<String, String> receivedPathParameters)
            throws HandlerRequestException {

        for (MessagePathParameter<?> pathParameter : messageParameters.getPathParameters()) {
            String value = receivedPathParameters.get(pathParameter.getKey());
            if (value != null) {
                try {
                    pathParameter.resolveFromString(value);
                } catch (Exception e) {
                    throw new HandlerRequestException(
                            "Cannot resolve path parameter ("
                                    + pathParameter.getKey()
                                    + ") from value \""
                                    + value
                                    + "\".");
                }
            }
        }
    }

    private static void resolveQueryParameters(
            MessageParameters messageParameters, Map<String, List<String>> receivedQueryParameters)
            throws HandlerRequestException {

        for (MessageQueryParameter<?> queryParameter : messageParameters.getQueryParameters()) {
            List<String> values = receivedQueryParameters.get(queryParameter.getKey());
            if (values != null && !values.isEmpty()) {
                StringJoiner joiner = new StringJoiner(",");
                values.forEach(joiner::add);

                try {
                    queryParameter.resolveFromString(joiner.toString());
                } catch (Exception e) {
                    throw new HandlerRequestException(
                            "Cannot resolve query parameter ("
                                    + queryParameter.getKey()
                                    + ") from value \""
                                    + joiner
                                    + "\".");
                }
            }
        }
    }

    private static <P extends MessageParameter<?>> Map<Class<? extends P>, P> mapParameters(
            Collection<P> parameters) {
        final Map<Class<? extends P>, P> mappedParameters =
                CollectionUtil.newHashMapWithExpectedSize(2);

        for (P parameter : parameters) {
            if (parameter.isResolved()) {
                @SuppressWarnings("unchecked")
                Class<P> clazz = (Class<P>) parameter.getClass();
                mappedParameters.put(clazz, parameter);
            }
        }

        return mappedParameters;
    }
}
