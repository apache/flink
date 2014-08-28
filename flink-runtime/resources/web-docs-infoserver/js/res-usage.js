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

// Declare global object as hook.
var flinkRU = {};

(function() {

	/** Container for the different profiling groups. */
	flinkRU.profilingGroups = [];

	// Helper functions
	flinkRU.helpers = {

		/** Creates a function that reads out the given property of events. */
		accessProperty : function(property) {
			return function(event) {
				return event[property];
			};
		},

		/** Options to be used with flot. */
		makeMemorySizeFormatter : function(baseScale) {
			return function(val, axis) {
				val *= baseScale
				if (val > 1000000)
					return (val / 1000000).toFixed(axis.tickDecimals) + " MB";
				else if (val > 1000)
					return (val / 1000).toFixed(axis.tickDecimals) + " kB";
				else
					return val.toFixed(axis.tickDecimals) + " B";
			}
		},

		makeInstanceFilter : function(instance) {
			return function(event) {
				return event.instanceName == instance;
			}
		}
	}

	// Configuring the profiling data to be displayed

	/**
	 * Template for profiling data.
	 */
	flinkRU.ProfilingSeries = function(label, number, evaluationFunction, additionalFilter) {

		this.lastTimestamp = 0;

		this.flotData = {
			color : number,
			label : label,
			data : []
		};

		this.evaluate = evaluationFunction;

		/** Updates a profiling series with the given profiling event. */
		this.updateWith = function(profilingEvent, property) {
			if (profilingEvent.timestamp > this.lastTimestamp && (additionalFilter == undefined || additionalFilter(profilingEvent))) {
				this.lastTimestamp = profilingEvent.timestamp;
				this.flotData.data.push([ profilingEvent.timestamp, this.evaluate(profilingEvent) ]);
			}
		}

		this.cutOffBefore = function(minTimestamp) {
			while (this.flotData.data.length > 0 && this.flotData.data[0][0] < minTimestamp) {
				this.flotData.data.shift();
			}
		}
	}

	/** Template for a set of profiling series to be plotted together. */
	flinkRU.ProfilingGroup = function(name, options, eventType, plotId, windowSize, additionalFilter) {

		this.name = name;
		this.options = options;
		this.eventType = eventType;
		this.plotId = plotId;
		this.series = [];
		this.windowSize = windowSize;

		this.updateWith = function(event) {
			if (event.type == this.eventType && (additionalFilter == undefined || additionalFilter(event))) {
				var lastTimestamp = -1;

				// Update all contained series with the event.
				for ( var i in this.series) {
					var ser = this.series[i];
					ser.updateWith(event);
					if (ser.lastTimestamp != undefined) {
						lastTimestamp = Math.max(lastTimestamp, ser.lastTimestamp);
					}
				}

				// Cut off too old data.
				if (lastTimestamp != -1 && this.windowSize >= 0) {
					var minTimestamp = lastTimestamp - this.windowSize;
					for ( var i in this.series) {
						var ser = this.series[i];
						ser.cutOffBefore(minTimestamp);
					}
				}
			}

		};

		this.plot = function() {
			var flotData = this.series.map(flinkRU.helpers.accessProperty("flotData"));
			$.plot(this.plotId, flotData, this.options);
		};
	}

	// Logic for displaying the profiling groups

	/**
	 * Polls profiling events and updates the plot.
	 */
	function pollResourceUsage() {
		$.ajax({
			url : "resourceUsage",
			cache : false,
			type : "GET",
			success : function(serverResponse) {
				updateData(serverResponse);
				plotData();
			},
			dataType : "json"
		});
	}

	/** Updates all the profiling series from an array of profiling events. */
	function updateData(serverResponse) {
		for ( var eventIndex in serverResponse) {
			var profilingEvent = serverResponse[eventIndex];

			for ( var groupIndex in flinkRU.profilingGroups) {
				var profilingGroup = flinkRU.profilingGroups[groupIndex];
				profilingGroup.updateWith(profilingEvent);
			}
		}
	}

	/** Plots all profiling groups. */
	function plotData() {
		for ( var groupIndex in flinkRU.profilingGroups) {
			var profilingGroup = flinkRU.profilingGroups[groupIndex];
			profilingGroup.plot();
		}
	}

	flinkRU.startPolling = function(interval) {
		pollResourceUsage();
		setInterval(pollResourceUsage, interval);
	};
})();
