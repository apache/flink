/*
 * Document initialization.
 */
$(document).ready(function () 
{

	
});

//The current JSON file
var JSONData; 
//The informations for the iterations
var iterationIds = new Array();
var iterationGraphs = new Array();
var iterationWidths = new Array();
var iterationHeights = new Array();

//Renders and draws the graph
function drawGraph(data, svgID){
	JSONData = data;
	
	//First step: precompute all iteration graphs
	
	//find all iterations
	iterationNodes = searchForIterationNodes();
	
	//add the graphs and the sizes + Ids to the arrays
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
	
	//new solution (with selection of id)
	var svgElement = d3.select(selector);
	layout = renderer.layout(layout).run(g, svgElement);
	
	 var svg = d3.select("#svg-main")
	 	.attr("width", $(document).width() - 15)
	 	.attr("height", $(document).height() - 15 - 110)
//	 	.attr("viewBox", "0 0 "+ ($(document).width() - 150) +" "+($(document).height() - 15 - 110))
	 	.call(d3.behavior.zoom("#svg-main").on("zoom", function() {
     		var ev = d3.event;
     		svg.select("#svg-main g")
     			.attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
  		}));
  		
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
	
	//stores all nodes that are in right graph -> no edges out of iterations!
	var existingNodes = new Array;
	
	var g = new dagreD3.Digraph();
	
	if (data.nodes != null) {
		console.log("Normal Json Data");
		for (var i in data.nodes) {
			var el = data.nodes[i];
			g.addNode(el.id, { label: createLabelNode(el, ""), nodesWithoutEdges: ""} );
			existingNodes.push(el.id);
			if (el.predecessors != null) {
				for (var j in el.predecessors) {
					if (existingNodes.indexOf(el.predecessors[j].id) != -1) {
						g.addEdge(null, el.predecessors[j].id, el.id, { label: createLabelEdge(el.predecessors[j]) });	
					} else {
						console.log("Edge to node not in graph yet: " + el.predecessors[j].id);
						var nWE = g.node(el.id).nodesWithoutEdges;
						if (nWE != "") {
							nWE += ", "+el.predecessors[j].id;
						} else {
							nWE = el.predecessors[j].id;
						}
						
						g.node(el.id, { label: createLabelNode(el, nWE), nodesWithoutEdges: nWE});
					}
				}
			}
		}
	} else {
		console.log("Iteration Json Data");
		for (var i in data.step_function) {
			var el = data.step_function[i];
			g.addNode(el.id, { label: createLabelNode(el, ""), nodesWithoutEdges: ""} );
			existingNodes.push(el.id);
			if (el.predecessors != null) {
				for (var j in el.predecessors) {
					if (existingNodes.indexOf(el.predecessors[j].id) != -1) {
						g.addEdge(null, el.predecessors[j].id, el.id, { label: createLabelEdge(el.predecessors[j]) });	
					} else {
						console.log("Edge to node not in graph yet: " + el.predecessors[j].id);
						var nWE = g.node(el.id).nodesWithoutEdges;
						if (nWE != "") {
							nWE += ", "+el.predecessors[j].id;
						} else {
							nWE = el.predecessors[j].id;
						}
						
						g.node(el.id, { label: createLabelNode(el, nWE), nodesWithoutEdges: nWE});
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
		if (el.local_strategy != undefined) {
			labelValue += ",<br>" + el.local_strategy;
		}
		labelValue += "</div>";
	}
	
	return labelValue;
}

//creates the label of a node, nWE are the NodesWithoutEdges
function createLabelNode(el, nWE) {
	var labelValue = "<div style=\"margin-top: 0\">";
	//set color of panel
	if (el.pact == "Data Source") {
		labelValue += "<div style=\"border-color:#4ce199; border-width:4px; border-style:solid\">";
	} else if (el.pact == "Data Sink") {
		labelValue += "<div style=\"border-color:#e6ec8b; border-width:4px; border-style:solid\">";
	} else {
		labelValue += "<div style=\"border-color:#3fb6d8; border-width:4px; border-style:solid\">";
	}
	//Nodename
	//New Solution with overlay
	labelValue += "<div><a nodeID=\""+el.id+"\" href=\"#\" rel=\"#propertyO\"><h3 style=\"text-align: center; font-size: 150%\">" + el.pact 
				+ " (ID = "+el.id+")</h3></a>";
	if (el.contents == "") {
		labelValue += "</div>";
	} else {
		var stepName = el.contents;
		//clean stepName
		stepName = shortenString(stepName);
	 	labelValue += "<h4 style=\"text-align: center; font-size: 130%\">" + stepName + "</h4></div>";
	}
	
	//If this node is a "iteration" we need a different panel-body
	if (el.step_function != null) {
		labelValue += extendLabelNodeForIteration(el.id);
		return labelValue;
	}
	
	if (el.parallelism != "") {
		labelValue += "<h5 style=\"font-size:115%\">Parallelism: " + el.parallelism + "</h5>";
	}

	if (el.driver_strategy != undefined) {
		labelValue += "<h5 style=\"font-size:115%\">Driver Strategy: " + el.driver_strategy + "</h5";
	}
	
	//Nodes without edges
	if (nWE != "") {
		labelValue += "<h5 style=\"font-size:115%\">Additional Edge to Node: <span class=\"badge\" style=\"font-size:120%\">"+nWE+"</span></h5>";
	}
	
	//close panel
	labelValue += "</div></div>";
	return labelValue;
}

//Extends the label of a node with an additional svg Element to present the workset iteration.
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

//presents properties for a given nodeID in the propertyCanvas
function showProperties(nodeID) {
	$("#propertyCanvas").empty();
	node = searchForNode(nodeID);
	var phtml = "<div><h3>Properties of "+ shortenString(node.contents) + " - ID = " + nodeID + "</h3>";
	phtml += "<div class=\"row\">";
	
	phtml += "<div class=\"col-md-4\"><h4>Pact Properties</h4>";
	phtml += "<table class=\"table\">";
	phtml += tableRow("Operator", (node.driver_strategy == undefined ? "None" : node.driver_strategy));
	phtml += tableRow("Parallelism", (node.parallelism == undefined ? "None" : node.parallelism));
	phtml += tableRow("Subtasks-per-instance", (node.subtasks_per_instance == undefined ? "None" : node.subtasks_per_instance));
	phtml += "</table></div>";
	
	phtml += "<div class=\"col-md-4\"><h4>Global Data Properties</h4>";
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

	phtml += "<div class=\"col-md-4\"><h4>Local Data Properties</h4>";
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
	phtml += "<div class=\"col-md-6\"><h4>Size Estimates</h4>";
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
	
	phtml += "<div class=\"col-md-6\"><h4>Cost Estimates</h4>";	
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

//Shortens a string to be shorter than 30 letters.
//If the string is an URL it shortens it in a way that "looks nice"
function shortenString(s) {
	var lastLength = s.length;
	do {
		lastLength = s.length;
		s = s.substring(s.indexOf("/")+1, s.length);
	} while (s.indexOf("/") != -1 && s.length > 30 && lastLength != s.length)
	//make sure that name does not contain a < (because of html)
	if (s.charAt(0) == "<") {
			s = s.replace("<", "&lt;");
			s = s.replace(">", "&gt;");
	}
	
	if (s.length > 30) {
		s = "..." + s.substring(s.length-30, s.length);
	}
	return s;
}