/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.externalresource.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** flink: Structure to serialize flink errors. */
public final class ErrorRecord {
    private String truncationMessage = "==Message truncated==";
    private String component;
    private String errorCode;
    private String message;
    private static final int terminationLogLengthLimit = 4096;

    /** flink ojectMapper for serDe. */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ErrorRecord() {}

    public ErrorRecord(String component, String errorCode, String message) {
        this.component = component;
        this.errorCode = errorCode;
        this.message = message;
    }

    public String getComponent() {
        return this.component;
    }

    public String getErrorCode() {
        return this.errorCode;
    }

    public String getMessage() {
        return this.message;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Truncate message to the length provided in input.
     *
     * @param length truncate to this length.
     */
    public void truncateMessage(int length) {
        if (this.message == null || this.message.length() <= length || length < 0) {
            return;
        }

        if (length == 0) {
            this.message = "";
            return;
        }

        // clear existing truncationMessage
        if (this.message.endsWith(truncationMessage)) {
            this.message =
                    this.message.substring(0, this.message.length() - truncationMessage.length());
        }

        if (length <= truncationMessage.length()) {
            this.message = this.message.substring(0, length);
            return;
        }

        this.message =
                this.message.substring(0, length - truncationMessage.length()) + truncationMessage;
    }

    /**
     * Serializas this instance as json string with limit in length.
     *
     * @param limit limit result string to this length.
     * @return String json.
     * @throws JsonProcessingException if serialization fails.
     */
    public String serialize(int limit) throws JsonProcessingException {
        String originalMessage = this.getMessage();
        this.setMessage("");
        int lengthWithOutMessage = objectMapper.writeValueAsString(this).length();
        this.setMessage(originalMessage);
        this.truncateMessage(limit - lengthWithOutMessage);
        String result = objectMapper.writeValueAsString(this);
        // overflow after truncation and serialization maybe caused by json serializer escaping
        // certain chars doubling them, / or ", thus truncate again by that overflow.
        int overflow = result.length() > limit ? result.length() - limit : 0;
        if (overflow > 0) {
            this.truncateMessage(this.getMessage().length() - overflow);
            result = objectMapper.writeValueAsString(this);
        }
        return result;
    }

    /**
     * Serializas this instance as json string with 4096 limit in length.
     *
     * @return String json.
     * @throws JsonProcessingException if serialization fails.
     */
    public String serialize() throws JsonProcessingException {
        return serialize(terminationLogLengthLimit);
    }
}
