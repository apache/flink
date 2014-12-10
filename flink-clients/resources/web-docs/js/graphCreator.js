/*!
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

/*
 * Document initialization.
 */
$(document).ready(function () 
{
	zoom = d3.behavior.zoom("#svg-main").on("zoom", function() {
     		var ev = d3.event;
     		d3.select("#svg-main g")
     			.attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
  		});
  	zoom.scaleExtent([0.3, 3])
});

//The current JSON file
var JSONData; 
//The informations for the iterations
var iterationIds = new Array();
var iterationGraphs = new Array();
var iterationWidths = new Array();
var iterationHeights = new Array();

//The zoom element
var zoom;
//informations about the enviroment
var svgWidth;
var svgHeight;

//Renders and draws the graph
function drawGraph(data, svgID){
	JSONData = data;
	
	//First step: precompute all iteration graphs
	
	//find all iterations
	iterationNodes = searchForIterationNodes();
	
	//add the graphs of iterations and their sizes + Ids to arrays
	if (iterationNodes != null) {
		for (var i in iterationNodes) {
			var itNode = iterationNodes[i];
			iterationIds.push(itNode.id);
			var g0 = loadJsonToDagre(itNode);
			iterationGraphs.push(g0);
		    var r = new dagreD3.Renderer();
		   	var l = dagreD3.layout()
		                 .nodeSep(20)
		                 .rankDir("LR");
		    l = r.layout(l).run(g0, d3.select("#svg-main"));
		
		   	iterationWidths.push(l._value.width);
			iterationHeights.push(l._value.height);
			
			//Clean svg
			$("#svg-main g").empty();
		}
	}
		
	//Continue normal
	var g = loadJsonToDagre(data);
	var selector = svgID + " g";
	var renderer = new dagreD3.Renderer();
	var layout = dagreD3.layout()
	                    .nodeSep(20)
	                    .rankDir("LR");
	var svgElement = d3.select(selector);
	layout = renderer.layout(layout).run(g, svgElement);
	
	svgHeight = layout._value.height;
	svgWidth = layout._value.width;
	
	 var svg = d3.select("#svg-main")
	 	.attr("width", $(document).width() - 15)
	 	.attr("height", $(document).height() - 15 - 110)
//	 	.attr("viewBox", "0 0 "+ ($(document).width() - 150) +" "+($(document).height() - 15 - 110))
	 	.call(zoom);
  		
	// This should now draw the precomputed graphs in the svgs... . 
	
	for (var i in iterationIds) {
		var workset = searchForNode(iterationIds[i]);
		renderer = new dagreD3.Renderer();
		layout = dagreD3.layout()
                    .nodeSep(20)
                    .rankDir("LR");
	    selector = "#svg-"+iterationIds[i]+" g";
	    svgElement = d3.select(selector);
	    layout = renderer.layout(layout).run(iterationGraphs[i], svgElement);
	}
  		
  	//enable Overlays and register function for overlay-infos
  	$("a[rel]").overlay({ 
		onBeforeLoad: function(){ 
   			var id = this.getTrigger().attr("nodeID")
			showProperties(id);
	 	}
  	});
  	
  	}

//Creates the dagreD3 graph object
//Responsible for adding nodes and edges
function loadJsonToDagre(data){
	
	//stores all nodes that are in current graph -> no edges to nodes which are outside of current iterations!
	var existingNodes = new Array;
	
	var g = new dagreD3.Digraph();
	
	//Find out whether we are in an iteration or in the normal json 
	//Bulk variables
	var partialSolution;
	var nextPartialSolution;
	//Workset variables
	var workset;
	var nextWorkset;
	var solutionSet;
	var solutionDelta;

	if (data.nodes != null) {
		//This is the normal json data
		var toIterate = data.nodes;
	} else {
		//This is an iteration, we now store special iteration nodes if possible
		var toIterate = data.step_function;
		partialSolution = data.partial_solution;
		nextPartialSolution = data.next_partial_solution;
		workset = data.workset;
		nextWorkset = data.next_workset;
		solutionSet = data.solution_set;
		solutionDelta = data.solution_delta;
	}
	
	for (var i in toIterate) {
		var el = toIterate[i];
		//create node, send additional informations about the node if it is a special one
		if (el.id == partialSolution) {
			g.addNode(el.id, { label: createLabelNode(el, "partialSolution"), nodesWithoutEdges: ""} );
		} else if (el.id == nextPartialSolution) {
			g.addNode(el.id, { label: createLabelNode(el, "nextPartialSolution"), nodesWithoutEdges: ""} );
		} else if (el.id == workset) {
			g.addNode(el.id, { label: createLabelNode(el, "workset"), nodesWithoutEdges: ""} );
		} else if (el.id == nextWorkset) {
			g.addNode(el.id, { label: createLabelNode(el, "nextWorkset"), nodesWithoutEdges: ""} );
		} else if (el.id == solutionSet) {
			g.addNode(el.id, { label: createLabelNode(el, "solutionSet"), nodesWithoutEdges: ""} );
		} else if (el.id == solutionDelta) {
			g.addNode(el.id, { label: createLabelNode(el, "solutionDelta"), nodesWithoutEdges: ""} );
		} else {
			g.addNode(el.id, { label: createLabelNode(el, ""), nodesWithoutEdges: ""} );
		}
		existingNodes.push(el.id);
		
		//create edgdes from predecessors to current node
		if (el.predecessors != null) {
			for (var j in el.predecessors) {
				if (existingNodes.indexOf(el.predecessors[j].id) != -1) {
					g.addEdge(null, el.predecessors[j].id, el.id, { label: createLabelEdge(el.predecessors[j]) });	
				} else {
					var missingNode = searchForNode(el.predecessors[j].id);
					if (missingNode.alreadyAdded != true) {
						missingNode.alreadyAdded = true;
						g.addNode(missingNode.id, {label: createLabelNode(missingNode, "mirror")});
						g.addEdge(null, missingNode.id, el.id, { label: createLabelEdge(missingNode) });
					}
				}
			}
		}
	}	
	return g;
}

//create a label of an edge
function createLabelEdge(el) {
	var labelValue = "";
	
	if (el.ship_strategy != null || el.local_strategy != null) {
		labelValue += "<div style=\"font-size: 100%; border:2px solid; padding:5px\">";
		if (el.ship_strategy != null) {
			labelValue += el.ship_strategy;
		}
		if (el.temp_mode != undefined) {
			labelValue += " (" + el.temp_mode + ")";
		}
		if (el.local_strategy != undefined) {
			labelValue += ",<br>" + el.local_strategy;
		}
		labelValue += "</div>";
	}
	
	return labelValue;
}

//creates the label of a node, in info is stored, whether it is a special node (like a mirror in an iteration)
function createLabelNode(el, info) {
//	if (info != "") {
//		console.log("The node " + el.id + " is a " + info);	
//	}
	
	//true, if the node is a special node from an iteration
	var specialIterationNode = (info == "partialSolution" || info == "nextPartialSolution" || info == "workset" || info == "nextWorkset" || info == "solutionSet" || info == "solutionDelta" );
	
	var labelValue = "<div style=\"margin-top: 0\">";
	//set color of panel
	if (info == "mirror") {
		labelValue += "<div style=\"border-color:#a8a8a8; border-width:4px; border-style:solid\">";
	} else if (specialIterationNode) {
		labelValue += "<div style=\"border-color:#CD3333; border-width:4px; border-style:solid\">";
	} else {
		//there is no info value, set normal color
		if (el.pact == "Data Source") {
			labelValue += "<div style=\"border-color:#4ce199; border-width:4px; border-style:solid\">";
		} else if (el.pact == "Data Sink") {
			labelValue += "<div style=\"border-color:#e6ec8b; border-width:4px; border-style:solid\">";
		} else {
			labelValue += "<div style=\"border-color:#3fb6d8; border-width:4px; border-style:solid\">";
		}
	}
	//Nodename
	if (info == "mirror") {
		labelValue += "<div><a nodeID=\""+el.id+"\" href=\"#\" rel=\"#propertyO\"><h3 style=\"text-align: center; "
		+ "font-size: 150%\">Mirror of " + el.pact + " (ID = "+el.id+")</h3></a>";
	} else {
		labelValue += "<div><a nodeID=\""+el.id+"\" href=\"#\" rel=\"#propertyO\"><h3 style=\"text-align: center; "
		+ "font-size: 150%\">" + el.pact + " (ID = "+el.id+")</h3></a>";
	}
	if (el.contents == "") {
		labelValue += "</div>";
	} else {
		var stepName = el.contents;
		//clean stepName
		stepName = shortenString(stepName);
	 	labelValue += "<h4 style=\"text-align: center; font-size: 130%\">" + stepName + "</h4></div>";
	}
	
	//If this node is an "iteration" we need a different panel-body
	if (el.step_function != null) {
		labelValue += extendLabelNodeForIteration(el.id);
	} else {
		//Otherwise add infos		
		if (specialIterationNode) {
			labelValue += "<h5 style=\"font-size:115%; text-align: center; color:#CD3333\">" + info + " Node</h5>";
		}
		
		if (el.parallelism != "") {
			labelValue += "<h5 style=\"font-size:115%\">Parallelism: " + el.parallelism + "</h5>";
		}
	
		if (el.driver_strategy != undefined) {
			labelValue += "<h5 style=\"font-size:115%\">Driver Strategy: " + el.driver_strategy + "</h5";
		}
		
	}
	//close divs
	labelValue += "</div></div>";
	return labelValue;
}

//Extends the label of a node with an additional svg Element to present the iteration.
function extendLabelNodeForIteration(id) {
	var svgID = "svg-" + id;

	//Find out the position of the iterationElement in the iterationGraphArray
	var index = iterationIds.indexOf(id);
	//Set the size and the width of the svg Element as precomputetd
	var width = iterationWidths[index] + 70;
	var height = iterationHeights[index] + 40;
	
	var labelValue = "<div id=\"attach\"><svg id=\""+svgID+"\" width="+width+" height="+height+"><g transform=\"translate(20, 20)\"/></svg></div>";
	return labelValue;
}

//presents properties for a given nodeID in the propertyCanvas overlay
function showProperties(nodeID) {
	$("#propertyCanvas").empty();
	node = searchForNode(nodeID);
	var phtml = "<div style='overflow-y: scroll; max-height:490px; overflow-x:hidden'><h3>Properties of "+ shortenString(node.contents) + " - ID = " + nodeID + "</h3>";
	phtml += "<div class=\"row\">";
	
	phtml += "<div class=\"col-sm-12\"><h4>Pact Properties</h4>";
	phtml += "<table class=\"table\">";
	phtml += tableRow("Operator", (node.driver_strategy == undefined ? "None" : node.driver_strategy));
	phtml += tableRow("Parallelism", (node.parallelism == undefined ? "None" : node.parallelism));
	phtml += tableRow("Subtasks-per-instance", (node.subtasks_per_instance == undefined ? "None" : node.subtasks_per_instance));
	phtml += "</table></div>";
	
	phtml += "<div class=\"col-sm-12\"><h4>Global Data Properties</h4>";
	phtml += "<table class=\"table\">";
	if (node.global_properties != null) {
		for (var i = 0; i < node.global_properties.length; i++) {
	    	var prop = node.global_properties[i];
	    	phtml += tableRow((prop.name == undefined ? '(unknown)' : prop.name),(prop.value == undefined ? "(none)" : prop.value));
	    }
	} else {
		phtml += tableRow("Global Properties", "None");
	}
	phtml += "</table></div>";

	phtml += "<div class=\"col-sm-12\"><h4>Local Data Properties</h4>";
	phtml += "<table class=\"table\">";
	if (node.local_properties != null) {
		for (var i = 0; i < node.local_properties.length; i++) {
			var prop = node.local_properties[i];
	     	phtml += tableRow((prop.name == undefined ? '(unknown)' : prop.name),(prop.value == undefined ? "(none)" : prop.value));
	    }
	} else {
		phtml += tableRow("Local Properties", "None");
	}
	phtml += "</table></div></div>";
	
	phtml += "<div class=\"row\">";
	phtml += "<div class=\"col-sm-12\"><h4>Size Estimates</h4>";
	phtml += "<table class=\"table\">";
	if (node.estimates != null) {
		for (var i = 0; i < node.estimates.length; i++) {
			var prop = node.estimates[i];
			phtml += tableRow((prop.name == undefined ? '(unknown)' : prop.name),(prop.value == undefined ? "(none)" : prop.value));
		}
	} else {
		phtml += tableRow("Size Estimates", "None");
	}
	phtml += "</table></div>";
	
	phtml += "<div class=\"col-sm-12\"><h4>Cost Estimates</h4>";	
	phtml += "<table class=\"table\">";
	if (node.costs != null) {
		for (var i = 0; i < node.costs.length; i++) {
	    	var prop = node.costs[i];
	    	phtml += tableRow((prop.name == undefined ? '(unknown)' : prop.name),(prop.value == undefined ? "(none)" : prop.value));
		}
	} else {
		phtml += tableRow("Cost Estimates", "None");
	}
	phtml += "</table></div>";
	
	phtml += "</div></div>";
	$("#propertyCanvas").append(phtml);
	
}

//searches in the global JSONData for the node with the given id
function searchForNode(nodeID) {
	for (var i in JSONData.nodes) {
		var el = JSONData.nodes[i];
		if (el.id == nodeID) {
			return el;
		}
		//look for nodes that are in iterations
		if (el.step_function != null) {
			for (var j in el.step_function) {
				if (el.step_function[j].id == nodeID) {
					return el.step_function[j];	
				}
			}	
		}
	}
}

//searches for all nodes in the global JSONData, that are iterations
function searchForIterationNodes() {
	var itN = new Array();
	for (var i in JSONData.nodes) {
		var el = JSONData.nodes[i];
		if (el.step_function != null) {
			itN.push(el);
		}
	}
	return itN;
}

//creates a row for a table with two collums
function tableRow(nameX, valueX) {
	var htmlCode = "";
	htmlCode += "<tr><td align=\"left\">" + nameX + "</td><td align=\"right\">" + valueX + "</td></tr>";
	return htmlCode;
}

//Split a string into multiple lines so that each line has less than 30 letters.
function shortenString(s) {
	//make sure that name does not contain a < (because of html)
	if (s.charAt(0) == "<") {
			s = s.replace("<", "&lt;");
			s = s.replace(">", "&gt;");
	}
	
	sbr = ""
	while (s.length > 30) {
		sbr = sbr + s.substring(0, 30) + "<br>"
		s = s.substring(30, s.length)
	}
	sbr = sbr + s

	return sbr;
}

//activates the zoom buttons
function activateZoomButtons() {
	$("#zoomIn").click(function() {
      	console.log("Clicked zoom in");
      	  if (zoom.scale() < 2.99) { 	
				var svg = d3.select("#svg-main");
				//Calculate and store new values in zoom object
				var translate = zoom.translate();
				var v1 = translate[0] * (zoom.scale() + 0.1 / (zoom.scale()));
				var v2 = translate[1] * (zoom.scale() + 0.1 / (zoom.scale()));
				zoom.scale(zoom.scale() + 0.1);
				zoom.translate([v1, v2]);
				//Transform svg
				svg.select("#svg-main g")
	     			.attr("transform", "translate(" + v1 + ","+ v2 + ") scale(" + zoom.scale() + ")");
      		}    	
      });
      
      $("#zoomOut").click(function() {
      		if (zoom.scale() > 0.31) { 	
				var svg = d3.select("#svg-main");
				//Calculate and store new values in zoom object
				zoom.scale(zoom.scale() - 0.1);
				var translate = zoom.translate();
				var v1 = translate[0] * (zoom.scale() - 0.1 / (zoom.scale()));
				var v2 = translate[1] * (zoom.scale() - 0.1 / (zoom.scale()));
				zoom.translate([v1, v2]);
				//Transform svg
				svg.select("#svg-main g")
	     			.attr("transform", "translate(" + v1 + ","+ v2 + ") scale(" + zoom.scale() + ")");
      		}
      });
}
