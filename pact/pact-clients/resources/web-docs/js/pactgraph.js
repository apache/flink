/*
 * global variable for the progress bar
 */
var progBar = null;

/*
 * Global variable that stores the pact plan as a map
 */
var idToObjectMap;   // map that maps IDs to the objects


/*
 * This functions handels the clicks to the fold button and expands the pact
 * box, showing the properties, or collapses it again.
 *
 * @param target The box that was clicked, as a jquery object.
 */
function handleFoldClick(target)
{
  var par = target.parent();

  var domNode = target[0];
  var domPar = par[0];
  
  var id = domPar.id.substr(9);
  
  var pactNode = idToObjectMap[id];
  var contDiv = $('#pactContents_' + id)[0];
  
  if (pactNode.expanded == true) {
    // fold
    domNode.innerHTML = "[+]";
    pactNode.expanded = false;
    
    domPar.style.minWidth = pactNode.oldMinWidth;
    domPar.style.minHeight = pactNode.oldMinHeight;
    domPar.style.width = pactNode.oldWidth;
    domPar.style.height = pactNode.oldHeight;
    domPar.style.maxWidth = pactNode.oldMaxWidth;
    domPar.style.maxHeight = pactNode.oldMaxHeight;
    
    domPar.style.top = pactNode.oldY;
    domPar.style.left = pactNode.oldX;

    contDiv.innerHTML = pactNode.oldContents;
  }
  else {
    // expand
    domNode.innerHTML = "[-]";
    pactNode.expanded = true;

    pactNode.oldMinWidth = domPar.style.minWidth;
    pactNode.oldMinHeight = domPar.style.minHeight;
    pactNode.oldWidth = domPar.style.width;
    pactNode.oldHeight = domPar.style.height;
    pactNode.oldMaxWidth = domPar.style.maxWidth;
    pactNode.oldMaxHeight = domPar.style.maxHeight;

    pactNode.oldY = domPar.style.top;
    pactNode.oldX = domPar.style.left;
    
    domPar.style.minWidth = "250px";
    domPar.style.minHeight = "80px";
    domPar.style.width = "auto";
    domPar.style.height = "auto";
    domPar.style.maxWidth = "300px";
    domPar.style.maxHeight = "300px";

    var pos = par.position();
    domPar.style.top = '' + (pos.top - 50) + 'px';
    domPar.style.left = '' + (pos.left - 100) + 'px';

    pactNode.oldContents = contDiv.innerHTML;
    
    var contTab = '<br/><br/><table class="propertiesTable" width="100%">';

    if (pactNode.local_strategy != undefined) {
      contTab += '<tr><td class="propertiesNameCell">Local Strategy</td><td class="propertiesValueCell">';
      contTab += pactNode.local_strategy;
      contTab += '</td></tr>';
    }

    if (pactNode.properties != undefined && pactNode.properties instanceof Array) {
      for (var i = 0; i < pactNode.properties.length; i++) {
        contTab += '<tr><td class="propertiesNameCell">';
        contTab += pactNode.properties[i].name;
        contTab += '</td><td class="propertiesValueCell">';
        contTab += pactNode.properties[i].value;
        contTab += '</td></tr>';
      }
    }

    contTab += '</table>';
    contDiv.innerHTML = contDiv.innerHTML + contTab;
  }

  // simulate a drag (by zero pixels) to trigger a graph update
  par.mousedown();
  par.mousemove();
  par.mouseup();
}

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
  cont += '<p class="propItem"><span class="propLabel">Output Contract: </span><span class="propValue">' + (node.outputcontract == undefined ? "None" : node.outputcontract) + '</span></p>';
  cont += '<p class="propItem"><span class="propLabel">Local Strategy: </span><span class="propValue">' + (node.local_strategy == undefined ? "None" : node.local_strategy) + '</span></p>';
  cont += '<p class="propItem"><span class="propLabel">Parallelism: </span><span class="propValue">' + (node.parallelism == undefined ? "None" : node.parallelism) + '</span></p>';
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
  if (node.properties != undefined && node.properties instanceof Array) {
    cont += '<div class="propertyCanvasSection">';
    cont += '<h4>Size Estimates</h4>';
    for (var i = 0; i < node.properties.length; i++) {
      var prop = node.properties[i];
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
  if (node.compiler_hints != undefined && node.compiler_hints instanceof Array) {
    cont += '<div class="propertyCanvasSection">';
    cont += '<h4>Compiler Hints</h4>';
    for (var i = 0; i < node.compiler_hints.length; i++) {
      var hint = node.compiler_hints[i];
      cont += '<p class="propItem"><span class="propLabel">' + (hint.name == undefined ? '(unknown)' : hint.name) + ':&nbsp&nbsp;&nbsp;</span><span class="propValue">' + 
        (hint.value == undefined ? "(none)" : hint.value) + '</span></p>';
    }
    cont += '</div>';
  }

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
  for (var k = 0; k < node.preRefs.length; k++)
  {
    topoVisit(node.preRefs[k], resultList);
  }
  // add the node to the list
  resultList.push(node);
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

  /* The remainder of the code works after the principle of best effort:
     it draws what is correct and ignores incorrect objects, not showing any error messages. */

  var nodeStr = '';                   // the markup string for the nodes
  var edgesStr = '';                  // the markup string for the edges

  var nodes = new Array();            // the array of valid nodes
  var danglingNodes = new Array();    // the array of invalid nodes

  // ----------------------------------------------------------------
  //   create the id->object map
  // ----------------------------------------------------------------
  
  idToObjectMap = new Object();
  for (var i = 0; i < plan.nodes.length; i++)
  {
    var node = plan.nodes[i];

    if (node != null) {
      if (node.id == undefined) {
        danglingNodes.push(node);
      }
      else {
        idToObjectMap[node.id] = node;
        nodes.push(node);
        node.preRefs = new Array();
        node.succRefs = new Array();
      }
    }
  }

  // ----------------------------------------------------------------
  //   clean up the predecessors and set the references in the objects
  // ----------------------------------------------------------------

  for (var i = 0; i < nodes.length; i++)
  {
    var node = nodes[i];
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

  // topologically sort the elements in the queue
  var sortedNodes = new Array();
  for (var i = 0; i < nodes.length; i++)
  {
    var node = nodes[i];
    topoVisit(node, sortedNodes);
  }
  nodes = sortedNodes;
  sortedNodes = null;

  // Compute the layout columns
  // This is easy: So through the topologically sorted list and
  // assign the nodes to columns based on their predecessors
  // we also assign the first naive row numbers, just as an
  // incrementing counter for rows in the columns
  
  var columns = new Array();
  for (var i = 0; i < nodes.length; i++)
  {
    var node = nodes[i];

    // check, if this node has predecessors
    // and assign it a column higher than the predecessors
    var maxCol = -1;
    for (var k = 0; k < node.preRefs.length; k++)
    {
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
      
    }
    else {
      node.layoutRow = columns[maxCol].length;
      columns[maxCol].push(node);
    }
  }
  
  // Compute the row layout: This is harder. Since we cannot assume a tree,
  // we have layout dependencies both an predecessors and successors.
  // Go back and forth, moving nodes between rows until it fits, maximally 10 iterations.
  // TODO!!!
  

  for (var i = 0; i < columns.length; i++)
  {
    var xMargin = 2 / (columns[i].length + 1);
    for (var k = 0; k < columns[i].length; k++)
    {
      var node = columns[i][k];
      
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
  
  
  // ----------------------------------------------------------------
  //   turn the row and column logical positons into actual positons
  // ----------------------------------------------------------------
  
  var halfHeight = Math.floor($('#mainCanvas').height() / 2);
  var columnWidth = Math.floor($('#mainCanvas').width() / columns.length);
  
  if (columnWidth > maxColumnWidth) {
    columnWidth = maxColumnWidth;
  }
  else if (columnWidth < minColumnWidth) {
    columnWidth = minColumnWidth;
  }
  
  for (var i = 0; i < columns.length; i++)
  {
    var xPos = Math.floor(i * columnWidth + columnWidth / 2);
    
    for (var k = 0; k < columns[i].length; k++)
    {
      var node = columns[i][k];
      node.xCenterPos = xPos;
      node.yCenterPos = Math.floor((1 + node.relativeYPos) * halfHeight);
    }
  }
  
  // ----------------------------------------------------------------
  //   create the markup for the graph
  // ----------------------------------------------------------------

  for (var i = 0; i < nodes.length; i++)
  {
    var node = nodes[i];
    var outputContr = node.outputcontract;

    // construct the string describing the current node
    if (node.type == "source" || node.type == "sink")
    {
      var type = node.type == "source" ? "datasource" : "datasink";
      var cont = node.contents == undefined ? "Unknown File" : node.contents;

      nodeStr += '<div class="block draggable ' + type + '" id="pactNode_' + node.id + '" style="left: ' + (node.xCenterPos - 50) + 'px; top: ' + (node.yCenterPos - 30) + 'px;">';
      nodeStr += '<div class="sourceSinkContents" id="pactContents_' + node.id + '"><span>' + node.contents + '</span></div>';
      nodeStr += '<div class="sourceSinkFold">[+]</div>';

      // if we have an output contract, add it and add also the container for the main contents
      if (outputContr != undefined) {
        nodeStr += '<div class="pactOutputContract"><span class="pactOutputContractContents">' + outputContr + '</span></div>';
      }
      nodeStr += '</div>';
    }
    else {
      // pact or unknown
      var pact = node.pact == undefined ? "Unknown PACT" : node.pact;
      var cont = node.contents == undefined ? "Unknown Function" : node.contents;

      nodeStr += '<div class="block draggable pact" id="pactNode_' + node.id + '" style="left: ' + (node.xCenterPos - 50) + 'px; top: ' + (node.yCenterPos - 30) + 'px;">';
      nodeStr += '<div class="pactTypeBox">' + pact + '</div>';
      nodeStr += '<div class="pactContents" id="pactContents_' + node.id + '">' + cont + '</div>';
      nodeStr += '<div class="pactFold">[+]</div>';

      // if we have an output contract, add it and add also the container for the main contents


      if (outputContr != undefined) {
        nodeStr += '<div class="pactOutputContract"><span class="pactOutputContractContents">' + outputContr + '</span></div>';
      }

        nodeStr += '</div>';
    }
    
    // add the edges
    if (node.predecessors != undefined && node.predecessors instanceof Array)
    { 
      for (var k = 0; k < node.predecessors.length; k++)
      {
        var pre = node.predecessors[k];
        if (pre.link != undefined)
        {
          // var dir = node.predecessors.length > 1 ? (pre.link.relativeYPos < node.relativeYPos ? "down_end" : "up_end") : "";
          
          // create the markup string for the edge
          edgesStr += '<div class="connector pactNode_' + pre.link.id + ' pactNode_' + node.id + ' right_start">' +
                        '<img src="img/arrows/' + imageFile + '" class="connector-end">';
          if (pre.channelType != undefined && pre.shippingStrategy != undefined) {
            edgesStr += '<label class="middle-label"><span class="channelType">' + pre.channelType + '</span><br/>' +
                                                    '<span class="shippingStrategy">(' + pre.shippingStrategy + ')</span>';
            if (pre.tempMode != undefined && pre.tempMode != null) {
              edgesStr += '<br/><span class="shippingStrategy">' + pre.tempMode + '</span>';
            }
            edgesStr += '</label>';
          }
          edgesStr += '</div>';
        }
      }
    }
  }

  // stop and erase the progress bar
  if (progBar != null) {
    progBar.Stop();
    progBar.Teardown();
    progBar = null;
  }

  // add the nodes and edges to the canvas
  var mc = $('#mainCanvas');
  mc.html('');
  mc.append(nodeStr);
  mc.append(edgesStr);

  // initialize the graph.it objects
  initPageObjects();
  
  if (propEvents) {
    // register the event handlers that react on the click on the '+'
    $('.pactFold').click(function () { handleFoldClick($(this))});
    $('.sourceSinkFold').click(function () { handleFoldClick($(this))});

    // register the event handlers that react on clicks on the box as it is
    $('.pact').click(function () { showProperties($(this))});
    $('.datasource').click(function () { showProperties($(this))});
    $('.datasink').click(function () { showProperties($(this))});
  }

  setTimeout("var all = $('.block');for (var i = 0; i < all.length; i++) { var obj = $(all[i]); obj.mousedown(); obj.mousemove(); obj.mouseup(); }", 50);
}
