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

// Helper functions
/** Creates a function that reads out the given property of events. */
function accessProperty(property) {
	return function(event) {
		return event[property];
	};
}

/** Options to be used with flot. */
function makeMemorySizeFormatter(baseScale) {
	return function(val, axis) {
		val *= baseScale
		if (val > 1000000)
			return (val / 1000000).toFixed(axis.tickDecimals) + " MB";
		else if (val > 1000)
			return (val / 1000).toFixed(axis.tickDecimals) + " kB";
		else
			return val.toFixed(axis.tickDecimals) + " B";
	}
}

// Configuring the profiling data to be displayed

/**
 * Template for profiling data.
 */
function ProfilingSeries(label, number, evaluationFunction) {

	this.lastTimestamp = 0;

	this.flotData = {
		color : number,
		label : label,
		data : []
	};

	this.evaluate = evaluationFunction;

	/** Updates a profiling series with the given profiling event. */
	this.updateWith = function(profilingEvent, property) {
		if (profilingEvent.timestamp > this.lastTimestamp) {
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

function ProfilingGroup(name, options, eventType, plotId, windowSize) {

	this.name = name;
	this.options = options;
	this.eventType = eventType;
	this.plotId = plotId;
	this.series = [];
	this.windowSize = windowSize;

	this.updateWith = function(event) {
		if (event.type == this.eventType) {
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
		var flotData = this.series.map(accessProperty("flotData"));
		$.plot(this.plotId, flotData, this.options);
	};
}

/** Container for the different profiling groups. */
var profilingGroups = [];

$(function initResourceUsage() {
	var windowSize = 30*1000; // 30 sec
	
	// CPU profiling
	var cpuFlotOptions = {
		xaxis : {
			mode : "time",
			timeformat : "%H:%M:%S"
		},
		yaxis : {
			min : 0,
			tickFormatter : function(val, axis) {
				return val.toFixed(axis.tickDecimals) + " %";
			}
		},
		series : {
			lines : {
				show : true,
				fill : 0.4
			},
			stack : true
		}

	};
	var cpuGroup = new ProfilingGroup("CPU", cpuFlotOptions, "InstanceSummaryProfilingEvent", "#cpuChart", windowSize);
	// add series in reverse stacking order
	cpuGroup.series.push(new ProfilingSeries("Interrupts", "#4682B4", function(e) {
		return e.softIrqCpu + e.hardIrqCpu
	}));
	cpuGroup.series.push(new ProfilingSeries("I/O Wait", "#FFFF00", accessProperty("ioWaitCpu")));
	cpuGroup.series.push(new ProfilingSeries("System CPU", "#8B0000", accessProperty("systemCpu")));
	cpuGroup.series.push(new ProfilingSeries("User CPU", "#006400", accessProperty("userCpu")));

	profilingGroups.push(cpuGroup);

	// Memory profiling
	var mainMemoryFlotOptions = {
		xaxis : {
			mode : "time",
			timeformat : "%H:%M:%S",
		},
		yaxis : {
			min : 0,
			// /proc/meminfo delivers kiB
			tickFormatter : makeMemorySizeFormatter(1024)
		},
		series : {
			lines : {
				show : true,
				fill : 0.4
			},
			stack : true
		}
	};
	var memGroup = new ProfilingGroup("Memory", mainMemoryFlotOptions, "InstanceSummaryProfilingEvent", "#memoryChart", windowSize);
	memGroup.series.push(new ProfilingSeries("Used memory", "#FF8C00", function(e) {
		return e.totalMemory - e.freeMemory;
	}));
	memGroup.series.push(new ProfilingSeries("Free memory", "#6495ED", accessProperty("freeMemory")));
	profilingGroups.push(memGroup);

	// Network profiling
	var networkMemoryFlotOptions = {
		xaxis : {
			mode : "time",
			timeformat : "%H:%M:%S",
		},
		yaxis : {
			min : 0,
			tickFormatter : makeMemorySizeFormatter(1)
		},
		series : {
			lines : {
				show : true,
				fill : 0.4
			}
		}
	};

	var networkGroup = new ProfilingGroup("Network", networkMemoryFlotOptions, "InstanceSummaryProfilingEvent", "#networkChart", windowSize);
	networkGroup.series.push(new ProfilingSeries("Transmitted", "#9370DB", accessProperty("transmittedBytes")));
	networkGroup.series.push(new ProfilingSeries("Received", "#BBBB00", accessProperty("receivedBytes")));
	profilingGroups.push(networkGroup)
});

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
		dataType : "json",
	});
};

/** Updates all the profiling series from an array of profiling events. */
function updateData(serverResponse) {
	for ( var eventIndex in serverResponse) {
		var profilingEvent = serverResponse[eventIndex];

		for ( var groupIndex in profilingGroups) {
			var profilingGroup = profilingGroups[groupIndex];
			profilingGroup.updateWith(profilingEvent);
		}
	}
};

/** Plots all profiling groups. */
function plotData() {
	for ( var groupIndex in profilingGroups) {
		var profilingGroup = profilingGroups[groupIndex];
		profilingGroup.plot();
	}
};

$(function startPolling() {
	pollResourceUsage();
	setInterval(pollResourceUsage, 1000);
});
