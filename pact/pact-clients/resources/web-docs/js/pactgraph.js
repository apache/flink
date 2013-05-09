var BULK_ITERATION_TYPE = "bulk_iteration";

/*
 * Global variable that stores the pact plan as a map
 */
var idToObjectMap;   // map that maps IDs to the objects

function getIterBoxHeight() {
    var $inspector = $("<div>").css('display', 'none').addClass('iteration-box');
    $("body").append($inspector); // add to DOM, in order to read the CSS property
    try {
        return $inspector.css('height');
    } finally {
        $inspector.remove(); // and remove from DOM
    }
};

function showProperties(target)
{
  var id = target[0].id.substr(9);
  var node = idToObjectMap[id];

  var nodeName = (node.contents == undefined ? "Unknown Function" : node.contents);
  var type = (node.type == "source") ? "(Data Source)" : (node.type == "sink") ? "(Data Sink)" : (node.pact == undefined) ? "(undefined)" : '(' + node.pact + ')';
  var cont = '<h3 style="text-align: left;">Properties of \'' + nodeName + '\' ' + type + ':</h3>';

  // column 1: output contract, local strategy, 
  cont += '<div class="propertyCanvasSection">';
  cont += '<h4>PACT Properties</h4>';
  cont += '<p class="propItem"><span class="propLabel">Operator: </span><span class="propValue">' + (node.driver_strategy == undefined ? "None" : node.driver_strategy) + '</span></p>';
  cont += '<p class="propItem"><span class="propLabel">Parallelism: </span><span class="propValue">' + (node.parallelism == undefined ? "None" : node.parallelism) + '</span></p>';
  cont += '<p class="propItem"><span class="propLabel">Subtasks-per-instance: </span><span class="propValue">' + (node.subtasks_per_instance == undefined ? "None" : node.subtasks_per_instance) + '</span></p>';
  cont += '</div>';

  // column 2: global properties
  if (node.global_properties != undefined && node.global_properties instanceof Array) {
    cont += '<div class="propertyCanvasSection">';
    cont += '<h4>Global Data Properties</h4>';
    for (var i = 0; i < node.global_properties.length; i++) {
      var prop = node.global_properties[i];
      cont += '<p class="propItem"><span class="propLabel">' + (prop.name == undefined ? '(unknown)' : prop.name) + ':&nbsp&nbsp;&nbsp;</span><span class="propValue">' + 
        (prop.value == undefined ? "(none)" : prop.value) + '</span></p>';
    }
    cont += '</div>';
  }

  // column 3: local properties
  if (node.local_properties != undefined && node.local_properties instanceof Array) {
    cont += '<div class="propertyCanvasSection">';
    cont += '<h4>Local Data Properties</h4>';
    for (var i = 0; i < node.local_properties.length; i++) {
      var prop = node.local_properties[i];
      cont += '<p class="propItem"><span class="propLabel">' + (prop.name == undefined ? '(unknown)' : prop.name) + ':&nbsp&nbsp;&nbsp;</span><span class="propValue">' + 
        (prop.value == undefined ? "(none)" : prop.value) + '</span></p>';
    }
    cont += '</div>';
  }

  // column 4: the other properties
  if (node.estimates != undefined && node.estimates instanceof Array) {
    cont += '<div class="propertyCanvasSection">';
    cont += '<h4>Size Estimates</h4>';
    for (var i = 0; i < node.estimates.length; i++) {
      var prop = node.estimates[i];
      cont += '<p class="propItem"><span class="propLabel">' + (prop.name == undefined ? '(unknown)' : prop.name) + ':&nbsp&nbsp;&nbsp;</span><span class="propValue">' + 
        (prop.value == undefined ? "(none)" : prop.value) + '</span></p>';
    }
    cont += '</div>';
  }

  // column 5: the costs
  if (node.costs != undefined && node.costs instanceof Array) {
    cont += '<div class="propertyCanvasSection">';
    cont += '<h4>Cost Estimates</h4>';
    for (var i = 0; i < node.costs.length; i++) {
      var prop = node.costs[i];
      cont += '<p class="propItem"><span class="propLabel">' + (prop.name == undefined ? '(unknown)' : prop.name) + ':&nbsp&nbsp;&nbsp;</span><span class="propValue">' + 
        (prop.value == undefined ? "(none)" : prop.value) + '</span></p>';
    }
    cont += '</div>';
  }

  // column 6: the compiler hints
//  if (node.compiler_hints != undefined && node.compiler_hints instanceof Array) {
//    cont += '<div class="propertyCanvasSection">';
//    cont += '<h4>Estimate Functions</h4>';
//    for (var i = 0; i < node.compiler_hints.length; i++) {
//      var hint = node.compiler_hints[i];
//      cont += '<p class="propItem"><span class="propLabel">' + (hint.name == undefined ? '(unknown)' : hint.name) + ':&nbsp&nbsp;&nbsp;</span><span class="propValue">' + 
//        (hint.value == undefined ? "(none)" : hint.value) + '</span></p>';
//    }
//    cont += '</div>';
//  }

  $('#propertyCanvas')[0].innerHTML = cont;
}

/*
 * This function is the visiting function of a depth-first-search
 * topological sort.
 *
 * @param node The current node.
 * @param result The array containing the topologically sorted list.
 */
function topoVisit(node, resultList)
{
  // check whether we have been here before
  if (node.topoVisited != undefined && node.topoVisited == true) {
    return;
  }
  // mark as visited
  node.topoVisited = true;
  // recursively descend
  for (var k = 0; k < node.preRefs.length; k++) {
    topoVisit(node.preRefs[k], resultList);
  }
  // add the node to the list
  resultList.push(node);
}

function topoSort(nodes)
{
  var sortedNodes = new Array();
  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    // recurse in case of an iteration
    if (node.step_function != undefined) {
      node.step_function = topoSort(node.step_function);
    }
    topoVisit(node, sortedNodes);
  }
  return sortedNodes;
}

function makeIdMap(arrayObj, allNodes) {
  for (var i = 0; i < arrayObj.length; i++)
  {
    var node = arrayObj[i];

    if (node != null) {
      if (node.id != undefined) {
        idToObjectMap[node.id] = node;
        allNodes.push(node);
        node.preRefs = new Array();
        node.succRefs = new Array();
        if (node.step_function != undefined) {
          // make the id map recursively for the step function
          makeIdMap(node.step_function, allNodes);
          
          // set the nodes for partial solutions, etc
          if (node.partial_solution != undefined) {
            var preNode = idToObjectMap[node.partial_solution];
            node.partial_solution_node = preNode;
          }
          if (node.next_partial_solution != undefined) {
            var preNode = idToObjectMap[node.next_partial_solution];
            node.next_partial_solution_node = preNode;
          }
          if (node.workset != undefined) {
            var preNode = idToObjectMap[node.workset];
            node.workset_node = preNode;
          }
          if (node.solution_set != undefined) {
            var preNode = idToObjectMap[node.solution_set];
            node.solution_set_node = preNode;
          }
          if (node.next_workset != undefined) {
            var preNode = idToObjectMap[node.next_workset];
            node.next_workset_node = preNode;
          }
          if (node.solution_delta != undefined) {
            var preNode = idToObjectMap[node.solution_delta];
            node.solution_delta_node = preNode;
          }
        }
      }
    }
  }
}

/*
 * Compute the layout columns
 * This is easy: Go through the topologically sorted list and
 * assign the nodes to columns based on their predecessors
 * we also assign the first naive row numbers, just as an
 * incrementing counter for rows in the columns
 */
function makeLogicalColumns(nodes)
{
  var columns = new Array();

  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    
    // recurse for iterations
    if (node.step_function != undefined) {
      node.columns = makeLogicalColumns(node.step_function);
    }

    // check, if this node has predecessors
    // and assign it a column higher than the predecessors
    var maxCol = -1;
    for (var k = 0; k < node.preRefs.length; k++) {
      var lc = node.preRefs[k].layoutColumn;
      if (lc != undefined && lc > maxCol) {
        maxCol = lc;
      }
    }
    maxCol++;
    node.layoutColumn = maxCol;

    if (maxCol >= columns.length) {
      node.layoutRow = 0;
      columns.push(new Array(node));
    } else {
      node.layoutRow = columns[maxCol].length;
      columns[maxCol].push(node);
    }
  }
  
  return columns;
}

/*
 * Compute the row layout: This is harder. Since we cannot assume a tree,
 * we have layout dependencies both an predecessors and successors.
 * Go back and forth, moving nodes between rows until it fits, maximally x iterations.
 * TODO!!!
 */
function makeLogicalRows(columns)
{
  for (var i = 0; i < columns.length; i++)
  {
    var xMargin = 2 / (columns[i].length + 1);
    for (var k = 0; k < columns[i].length; k++)
    {
      var node = columns[i][k];
      if (node.step_function != undefined) {
        makeLogicalRows(node.columns);
      }
      
      if (node.preRefs.length == 0) {
        // case no predecessors. place at the relative position with respect to the
        // number of nodes in the column
        node.relativeYPos = (node.layoutRow + 1) * xMargin - 1;
      }
      else if (node.preRefs.length > 1) {
        // case we have a node with multiple inputs.
        // place in the middle between the two predecessors
        var mean = 0;
        for (var z = 0; z < node.preRefs.length; z++) {
          mean += node.preRefs[z].relativeYPos; 
        }
        node.relativeYPos = mean / node.preRefs.length;
      }
      else {
        // case one predecessor, which may feed into other
        // nodes besides this one.
        var pred = node.preRefs[0];
        
        if (pred.succRefs.length == 1) {
          // case this node is the only successor
          // this node is on the same level then
          node.relativeYPos = pred.relativeYPos;
        }
        else {
          var divertation = 2 / Math.max(columns[i].length + 1, columns[i-1].length + 2);
        
          if (node.layoutRow <= pred.layoutRow) {
            node.relativeYPos = pred.relativeYPos - divertation;
          }
          else {
            node.relativeYPos = pred.relativeYPos + divertation;
          }
        }
      }
    }
  }
}

function countColumns(nodes) {
  var num = 0;
  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    if (node.columns != undefined && node.columns.length != undefined) {
      num += node.columns.length;
    }
  }
  return num;
}

function makePhysicalPositions(columns, xPos, halfHeight, columnWidth, iterHeight) {
  var totalColumnSkips = 0;
  for (var i = 0; i < columns.length; i++) {
    var columnSkips = 1;
    for (var k = 0; k < columns[i].length; k++) {
      var node = columns[i][k];
      node.xCenterPos = xPos;
      node.yCenterPos = Math.floor((1 + node.relativeYPos) * halfHeight);
      
      if (node.columns != undefined) {
        var skips = makePhysicalPositions(node.columns, columnWidth / 2, iterHeight / 2, columnWidth, iterHeight);
        node.iter_width = skips * columnWidth;
        if (skips > columnSkips) {
          columnSkips = skips;
        }
      }
    }
    xPos = Math.floor(xPos + (columnSkips * columnWidth));
    totalColumnSkips += columnSkips;
  }
  return totalColumnSkips;
}


function drawNode(node, nodeStr, columnWidth) {
  // construct the string describing the current node
  if (node.type == "source" || node.type == "sink") {
    var type = node.type == "source" ? "datasource" : "datasink";
    var cont = node.contents == undefined ? "Unknown" : node.contents;
 
    nodeStr += '<div class="block draggable ' + type + '" id="pactNode_' + node.id + '" style="left: ' + (node.xCenterPos - 50) + 'px; top: ' + (node.yCenterPos - 30) + 'px;">';
    nodeStr += '<div class="sourceSinkContents"><span>' + node.contents + '</span></div>';
    nodeStr += '</div>';
  } else if (node.type == BULK_ITERATION_TYPE) {
    var cont = node.contents == undefined ? "Unnamed Bulk Iteration" : node.contents;
    var partialSolution = node.partial_solution_node;
    var nextPartial = node.next_partial_solution_node;
    nodeStr += '<div class="block draggable iteration-box" id="pactNode_' + node.id + '" style="left: ' + (node.xCenterPos - (columnWidth / 2)) + 'px; top: ' + (node.yCenterPos - (node.iter_height/2)) + 'px; width: ' + node.iter_width + 'px;">';
    nodeStr += '<div class="iteration-name-label">' + cont + '</div>';
    nodeStr += '<div class="block iteration-set-box iteration-set-box-left" id="pactNode_' + node.partial_solution + '" style="top: ' + (partialSolution.yCenterPos - 15) + 'px; vertical-align: middle;"></div>';
    nodeStr += '<div class="block iteration-set-box iteration-set-box-right" id="pactNode_' + node.id + '_ns" style="top: ' + (nextPartial.yCenterPos - 15) + 'px; vertical-align: middle;"></div>';

    
    for (var i = 0; i < node.step_function.length; i++) {
      var child = node.step_function[i];
      if (child.type != undefined && child.pact != "Bulk Partial Solution") {
        nodeStr = drawNode(child, nodeStr, columnWidth);
      }
    }
    
    nodeStr += '</div>';
  } else {
    // pact or unknown
    var pact = node.pact == undefined ? "Unknown PACT" : node.pact;
    var cont = node.contents == undefined ? "Unknown Function" : node.contents;

    nodeStr += '<div class="block draggable pact" id="pactNode_' + node.id + '" style="left: ' + (node.xCenterPos - 50) + 'px; top: ' + (node.yCenterPos - 30) + 'px;">';
    nodeStr += '<div class="pactTypeBox">' + pact + '</div>';
    nodeStr += '<div class="pactContents">' + cont + '</div>';
    nodeStr += '</div>';
  }
  return nodeStr;
}

function drawEdge(edgesStr, pre, preId, postId, imageFile) {
  // create the markup string for the edge
  edgesStr += '<div class="connector pactNode_' + preId + ' pactNode_' + postId + ' right_start">' +
              '<img src="img/arrows/' + imageFile + '" class="connector-end">';
  if (pre.ship_strategy != undefined || pre.local_strategy != undefined || pre.temp_mode != undefined) {
    edgesStr += '<label class="middle-label">';
    if (pre.ship_strategy != undefined) {
      edgesStr += '<span class="shippingStrategy">' + pre.ship_strategy + '</span><br/>';
    }
    if (pre.local_strategy != undefined) {
      edgesStr += '<span class="localStrategy">' + pre.local_strategy + '</span><br/>';
    }
    if (pre.temp_mode != undefined) {
      edgesStr += '<span class="cacheStrategy">' + pre.temp_mode + '</span><br/>';
    }
    edgesStr += '</label>';
  }
  edgesStr += '</div>';
  
  return edgesStr;
}

/*
 * Process a returned pact plan. This function has several
 * steps:
 * 
 *  - Clean the returned plan
 *  - Compute the graph layout (logically)
 *  - Create the graph markup strings
 *  - Turn off the progress bar
 *  - Add the graph to the canvas (invisible)
 *  - Compute and set the actual positions in the window.
 */
function drawPactPlan(plan, propEvents, imageFile)
{
  // initial sanity check
  if (plan == null || plan.nodes == undefined || (!(plan.nodes instanceof Array))) {
    alert('The retrieved data does not represent a valid PACT plan.');
    return;
  }
  
  var iterHeight = getIterBoxHeight();
  if (iterHeight != null && iterHeight.substring != undefined && iterHeight.length > 2 && iterHeight.substring(iterHeight.length - 2, iterHeight.length) == 'px') {
    iterHeight = iterHeight.substring(0, iterHeight.length - 2);
  } else {
    iterHeight = 350;
  }

  /* The remainder of the code works after the principle of best effort:
     it draws what is correct and ignores incorrect objects, not showing any error messages. */

  var nodeStr = '';                   // the markup string for the nodes
  var edgesStr = '';                  // the markup string for the edges

  var nodes = plan.nodes            // the array of valid nodes
  var allNodes = new Array();    // the array of invalid nodes

  // ----------------------------------------------------------------
  //   create the id->object map
  // ----------------------------------------------------------------
  
  idToObjectMap = new Object();
  makeIdMap(nodes, allNodes);

  // ----------------------------------------------------------------
  //   clean up the predecessors and set the references in the objects
  // ----------------------------------------------------------------

  for (var i = 0; i < allNodes.length; i++)
  {
    var node = allNodes[i];
    
    if (node.type == BULK_ITERATION_TYPE) {
      node.iter_height = iterHeight;
    }
    
    if (node.predecessors != undefined && node.predecessors instanceof Array)
    {
      for (var k = 0; k < node.predecessors.length; k++)
      {
        var pre = node.predecessors[k];
        if (pre.id != undefined)
        {
          var preNode = idToObjectMap[pre.id];
          if (preNode != undefined && preNode != null) {
            node.preRefs.push(preNode);
            preNode.succRefs.push(node);
            pre.link = preNode;
          }
        }
      }
    }
  }

  // ----------------------------------------------------------------
  //   compute the visual layout of the graph
  // ----------------------------------------------------------------

  // topologically sort the nodes in the array
  nodes = topoSort(nodes);

  // Compute the layout columns
  var columns = makeLogicalColumns(nodes);
  
  // Compute the row layout
  makeLogicalRows(columns);
  
  // ----------------------------------------------------------------
  //   turn the row and column logical positons into actual positons
  // ----------------------------------------------------------------
  
  var numCols = columns.length + countColumns(nodes);
  var halfHeight = Math.floor($('#mainCanvas').height() / 2);
  var columnWidth = Math.floor($('#mainCanvas').width() / numCols);
  
  if (columnWidth > maxColumnWidth) {
    columnWidth = maxColumnWidth;
  }
  else if (columnWidth < minColumnWidth) {
    columnWidth = minColumnWidth;
  }
  
  var xPos = Math.floor(columnWidth / 2);
  makePhysicalPositions(columns, xPos, halfHeight, columnWidth, iterHeight);
  
  // ----------------------------------------------------------------
  //   create the markup for the graph
  // ----------------------------------------------------------------

  for (var i = 0; i < nodes.length; i++) {
    nodeStr = drawNode(nodes[i], nodeStr, columnWidth);
  }

  for (var i = 0; i < allNodes.length; i++) {
    var node = allNodes[i];
    // add the edges
    if (node.predecessors != undefined && node.predecessors instanceof Array) { 
      for (var k = 0; k < node.predecessors.length; k++) {
        var pre = node.predecessors[k];
        if (pre.link != undefined) {
          
          var preId = pre.link.id;
          var postId = node.id;
          
          if (node.type == BULK_ITERATION_TYPE) {
            postId = node.partial_solution;
            // create the edge from the partial solution root to the box anchor
            edgesStr = drawEdge(edgesStr, node.next_partial_solution_node, node.next_partial_solution, node.id + '_ns', imageFile);
          }
          if (pre.link.type == BULK_ITERATION_TYPE) {
            preId = pre.link.id + '_ns';
          }
          
          edgesStr = drawEdge(edgesStr, pre, preId, postId, imageFile);
        }
      }
    }
  }

  // add the nodes and edges to the canvas
  var mc = $('#mainCanvas');
  mc.html('');
  mc.append(nodeStr);
  mc.append(edgesStr);

  // initialize the graph.it objects
  initPageObjects();
  
  if (propEvents) {
    // register the event handlers that react on clicks on the box as it is
    $('.pact').click(function () { showProperties($(this))});
    $('.iteration-box').click(function () { showProperties($(this))});
    $('.datasource').click(function () { showProperties($(this))});
    $('.datasink').click(function () { showProperties($(this))});
  }

  setTimeout("var all = $('.block');for (var i = 0; i < all.length; i++) { var obj = $(all[i]); obj.mousedown(); obj.mousemove(); obj.mouseup(); }", 50);
}
