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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.graph.StreamEdge;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The interface for input selection.
 */
public interface InputSelector {

	/**
	 * The enum Input type.
	 */
	enum InputType {
		/**
		 * Source input type.
		 */
		SOURCE,
		/**
		 * Edge input type.
		 */
		EDGE
	}

	/**
	 * Register listener.
	 *
	 * @param listener the listener
	 */
	void registerSelectionChangedListener(SelectionChangedListener listener);

	/**
	 * Gets next selected inputs.
	 *
	 * @return the next selected inputs
	 */
	List<InputSelection> getNextSelectedInputs();

	/**
	 * The interface Input selection.
	 */
	interface InputSelection {

		/**
		 * Gets input type.
		 *
		 * @return the input type
		 */
		InputType getInputType();

		/**
		 * Convert to source input selection.
		 *
		 * @return the source input selection
		 */
		SourceInputSelection toSourceInputSelection();

		/**
		 * Convert to edge input selection.
		 *
		 * @return the edge input selection
		 */
		EdgeInputSelection toEdgeInputSelection();

	}

	/**
	 * The Source input selection.
	 */
	class SourceInputSelection implements InputSelection {

		private final int sourceOperatorId;

		private SourceInputSelection(int sourceOperatorId) {
			this.sourceOperatorId = sourceOperatorId;
		}

		/**
		 * Gets source operator id.
		 *
		 * @return the source operator id
		 */
		public int getSourceOperatorId() {
			return sourceOperatorId;
		}

		@Override
		public InputType getInputType() {
			return InputType.SOURCE;
		}

		@Override
		public SourceInputSelection toSourceInputSelection() {
			return this;
		}

		@Override
		public EdgeInputSelection toEdgeInputSelection() {
			throw new UnsupportedOperationException("This is not an EdgeInputSelection");
		}

		@Override
		public int hashCode() {
			return Objects.hash(InputType.SOURCE, sourceOperatorId);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (obj != null && obj.getClass() == getClass()) {
				final SourceInputSelection other = (SourceInputSelection) obj;
				return other.getSourceOperatorId() == sourceOperatorId;
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this).
				append("InputType", InputType.SOURCE).
				append("sourceOperatorId", sourceOperatorId).
				toString();
		}

		/**
		 * Create source input selection.
		 *
		 * @param sourceOperatorId the source operator id
		 * @return the source input selection
		 */
		public static SourceInputSelection create(int sourceOperatorId) {
			return new SourceInputSelection(sourceOperatorId);
		}
	}

	/**
	 * The Edge input selection.
	 */
	class EdgeInputSelection implements InputSelection {

		private final StreamEdge streamEdge;

		private EdgeInputSelection(StreamEdge streamEdge) {
			this.streamEdge = checkNotNull(streamEdge);
		}

		/**
		 * Gets stream edge.
		 *
		 * @return the stream edge
		 */
		public StreamEdge getStreamEdge() {
			return streamEdge;
		}

		@Override
		public InputType getInputType() {
			return InputType.EDGE;
		}

		@Override
		public SourceInputSelection toSourceInputSelection() {
			throw new UnsupportedOperationException("This is not a SourceInputSelection");
		}

		@Override
		public EdgeInputSelection toEdgeInputSelection() {
			return this;
		}

		/**
		 * Create edge input selection.
		 *
		 * @param streamEdge the stream edge
		 * @return the edge input selection
		 */
		public static EdgeInputSelection create(StreamEdge streamEdge) {
			return new EdgeInputSelection(streamEdge);
		}

		@Override
		public int hashCode() {
			return Objects.hash(streamEdge);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (obj != null && obj.getClass() == getClass()) {
				final EdgeInputSelection other = (EdgeInputSelection) obj;
				return other.getStreamEdge().equals(streamEdge);
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this).
				append("streamEdge", streamEdge).
				toString();
		}
	}

	/**
	 * The interface Selection changed listener.
	 */
	interface SelectionChangedListener {
		/**
		 * Notify selection changed.
		 */
		void notifySelectionChanged();
	}
}
