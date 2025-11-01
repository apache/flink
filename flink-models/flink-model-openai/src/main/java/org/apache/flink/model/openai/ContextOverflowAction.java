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

package org.apache.flink.model.openai;

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingResult;
import com.knuddels.jtokkit.api.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.configuration.description.TextElement.text;

/** Context overflow action. */
public enum ContextOverflowAction implements DescribedEnum {
    TRUNCATED_TAIL("truncated-tail", "Truncates exceeded tokens from the tail of the context.") {
        @Override
        public String processTokensWithLimitInternal(
                Encoding encoding, String input, int maxContextSize, int actualNumTokens) {
            EncodingResult encodingResult = encoding.encodeOrdinary(input, maxContextSize);
            return encoding.decode(encodingResult.getTokens());
        }
    },
    TRUNCATED_TAIL_LOG(
            "truncated-tail-log",
            "Truncates exceeded tokens from the tail of the context. Records the truncation log.") {
        @Override
        public String processTokensWithLimitInternal(
                Encoding encoding, String input, int maxContextSize, int actualNumTokens) {
            LOG.info(
                    "Context overflowed (threshold: {}, actual: {}), truncating the tail of input.",
                    maxContextSize,
                    actualNumTokens);
            EncodingResult encodingResult = encoding.encodeOrdinary(input, maxContextSize);
            return encoding.decode(encodingResult.getTokens());
        }
    },
    TRUNCATED_HEAD("truncated-head", "Truncates exceeded tokens from the head of the context.") {
        @Override
        public String processTokensWithLimitInternal(
                Encoding encoding, String input, int maxContextSize, int actualNumTokens) {
            IntArrayList tokens = encoding.encodeOrdinary(input);
            return encoding.decode(
                    new HeadTrimmedIntArrayList(tokens, tokens.size() - maxContextSize));
        }
    },
    TRUNCATED_HEAD_LOG(
            "truncated-head-log",
            "Truncates exceeded tokens from the head of the context. Records the truncation log.") {
        @Override
        public String processTokensWithLimitInternal(
                Encoding encoding, String input, int maxContextSize, int actualNumTokens) {
            LOG.info(
                    "Context overflowed (threshold: {}, actual: {}), truncating the head of input.",
                    maxContextSize,
                    actualNumTokens);
            IntArrayList tokens = encoding.encodeOrdinary(input);
            return encoding.decode(
                    new HeadTrimmedIntArrayList(tokens, tokens.size() - maxContextSize));
        }
    },
    SKIPPED("skipped", "Skips the input row.") {
        @Nullable
        @Override
        public String processTokensWithLimitInternal(
                Encoding encoding, String input, int maxContextSize, int actualNumTokens) {
            return null;
        }
    },
    SKIPPED_LOG("skipped-log", "Skips the input row. Records the skipping log.") {
        @Nullable
        @Override
        public String processTokensWithLimitInternal(
                Encoding encoding, String input, int maxContextSize, int actualNumTokens) {
            LOG.info(
                    "Context overflowed (threshold: {}, actual: {}), skipping input.",
                    maxContextSize,
                    actualNumTokens);
            return null;
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(ContextOverflowAction.class);

    private static final EncodingRegistry ENCODING_REGISTRY = Encodings.newLazyEncodingRegistry();
    private static final Map<String, Encoding> ENCODING_MAP = new ConcurrentHashMap<>();

    private final String value;
    private final String description;

    ContextOverflowAction(String value, String description) {
        this.value = value;
        this.description = description;
    }

    /**
     * Initialize encoding for given model.
     *
     * @param model The model to calculate tokens.
     * @param maxContextSize the context limit.
     */
    public void initializeEncodingForContextLimit(String model, @Nullable Integer maxContextSize) {
        if (maxContextSize == null) {
            return;
        }

        Optional<Encoding> optionalEncoding =
                Encodings.newLazyEncodingRegistry().getEncodingForModel(model);
        if (optionalEncoding.isPresent()) {
            ENCODING_MAP.putIfAbsent(model, optionalEncoding.get());
            return;
        }

        throw new IllegalArgumentException(
                String.format(
                        "No proper tokenizer found for model %s. Context size cannot be set.",
                        model));
    }

    /**
     * Process tokens with given context limit.
     *
     * @param model The model to calculate tokens.
     * @param input The input string.
     * @param maxContextSize the context limit.
     * @return processed tokens or null if the tokens should be skipped.
     */
    public @Nullable String processTokensWithLimit(
            String model, String input, @Nullable Integer maxContextSize) {
        if (maxContextSize == null) {
            return input;
        }

        Encoding encoding = ENCODING_MAP.get(model);
        int actualNumTokens = encoding.countTokensOrdinary(input);
        if (actualNumTokens <= maxContextSize) {
            return input;
        }

        return processTokensWithLimitInternal(encoding, input, maxContextSize, actualNumTokens);
    }

    abstract @Nullable String processTokensWithLimitInternal(
            Encoding encoding, String input, int maxContextSize, int actualNumTokens);

    @Override
    public InlineElement getDescription() {
        return text(description);
    }

    @Override
    public String toString() {
        return value;
    }

    public static String getAllValuesAndDescriptions() {
        StringBuilder sb = new StringBuilder();
        int index = 1;
        for (ContextOverflowAction action : values()) {
            sb.append(index)
                    .append(". ")
                    .append(action.value)
                    .append(":\t")
                    .append(action.description)
                    .append("\n");
            index++;
        }
        return sb.toString();
    }

    private static class HeadTrimmedIntArrayList extends IntArrayList {
        private final IntArrayList delegate;
        private int headOffset;

        private HeadTrimmedIntArrayList(IntArrayList delegate, int headOffset) {
            this.delegate = delegate;
            this.headOffset = headOffset;
        }

        @Override
        public void clear() {
            this.delegate.clear();
            this.headOffset = 0;
        }

        @Override
        public void add(int element) {
            delegate.add(element);
        }

        @Override
        public int get(int index) {
            return delegate.get(index + headOffset);
        }

        @Override
        public int set(int index, int element) {
            return delegate.set(index + headOffset, element);
        }

        @Override
        public void ensureCapacity(int minCapacity) {
            delegate.ensureCapacity(minCapacity + headOffset);
        }

        @Override
        public int size() {
            return delegate.size() - headOffset;
        }

        @Override
        public boolean isEmpty() {
            return delegate.size() == headOffset;
        }

        @Override
        public int[] toArray() {
            return Arrays.copyOfRange(delegate.toArray(), headOffset, delegate.size());
        }

        @Override
        public List<Integer> boxed() {
            return super.boxed().subList(headOffset, delegate.size());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HeadTrimmedIntArrayList)) {
                return false;
            }
            HeadTrimmedIntArrayList other = (HeadTrimmedIntArrayList) o;
            return Objects.equals(delegate, other.delegate) && headOffset == other.headOffset;
        }

        @Override
        public int hashCode() {
            return 31 * delegate.hashCode() + headOffset;
        }

        @Override
        public String toString() {
            return this.boxed().toString();
        }
    }
}
