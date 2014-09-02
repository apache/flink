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

$(document).ready(function() {
	pollTaskmanagers();
	
});

/*
 * Initializes taskmanagers table
 */
function loadTaskmanagers(json) {
	$("#taskmanagerTable").empty();
	var table = "<table class=\"table table-bordered table-hover table-striped\">";
	table += "<tr><th>Node</th><th>Ipc Port</th><th>Data Port</th><th>Seconds since last Heartbeat</th>" +
			"<th>Number of Slots</th><th>Available Slots</th><th>CPU Cores</th><th>Physical Memory (mb)</th><th>TaskManager Heapsize (mb)</th></tr>";
	for (var i = 0; i < json.taskmanagers.length; i++) {
		var tm = json.taskmanagers[i]
		table += "<tr><td>"+tm.inetAdress+"</td><td>"+tm.ipcPort+"</td><td>"+tm.dataPort+"</td><td>"+tm.timeSinceLastHeartbeat+"</td>" +
				"<td>"+tm.slotsNumber+"</td><td>"+tm.freeSlots+"</td><td>"+tm.cpuCores+"</td><td>"+tm.physicalMemory+"</td><td>"+tm.freeMemory+"</td></tr>";
	}
	table += "</table>";
	$("#taskmanagerTable").append(table);
}

function pollTaskmanagers() {
	$.ajax({ url : "setupInfo?get=taskmanagers", type : "GET", cache: false, success : function(json) {
		loadTaskmanagers(json);
	}, dataType : "json",
	});
	setTimeout(function() {
		pollTaskmanagers();
	}, 10000);
}
