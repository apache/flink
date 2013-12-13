/*********************
 * browser detection *
 *********************/

var ie=document.all;
var nn6=document.getElementById&&!document.all;

/****************************************************
 * This class is a scanner for the visitor pattern. *
 ****************************************************/
 
/**
 * Constructor, parameters are:
 * visitor: the visitor implementation, it must be a class with a visit(element) method.
 * scanElementsOnly: a flag telling whether to scan html elements only or all html nodes.
 */
function DocumentScanner(visitor, scanElementsOnly)
{
	this.visitor = visitor;
	this.scanElementsOnly = scanElementsOnly;

	/**
	 * Scans the element
	 */
	this.scan = function(element)
	{
		var i;
		if(this.visitor.visit(element))
		{
			// visit child elements
			var children = element.childNodes;
			for(i = 0; i < children.length; i++)
			{
				if(!this.scanElementsOnly || children[i].nodeType == 1)
				{
					this.scan(children[i]);
				}
			}
		}		
	}	
}

/*****************
 * drag and drop *
 *****************/
 
var isdrag=false;					// this flag indicates that the mouse movement is actually a drag.
var mouseStartX, mouseStartY;		// mouse position when drag starts
var elementStartX, elementStartY;	// element position when drag starts

/**
 * the html element being dragged.
 */
var elementToMove;

/**
 * an array containing the blocks being dragged. This is used to notify them of move.
 */
var blocksToMove;

/**
 * this variable stores the orginal z-index of the object being dragged in order
 * to restore it upon drop.
 */ 
var originalZIndex;

/**
 * an array containing bounds to be respected while dragging elements,
 * these bounds are left, top, left + width, top + height of the parent element.
 */
var bounds = new Array(4);

/**
 * this visitor is used to find blocks nested in the element being moved.
 */
function BlocksToMoveVisitor()
{
	this.visit = function(element)
	{
		if(isBlock(element))
		{
			blocksToMove.push(findBlock(element.id));
			return false;
		}
		else
			return true;
	}
}

var blocksToMoveScanner = new DocumentScanner(new BlocksToMoveVisitor(), true);

function movemouse(e)
{
	if (isdrag)
	{
		var currentMouseX = nn6 ? e.clientX : event.clientX;
		var currentMouseY = nn6 ? e.clientY : event.clientY;
		var newElementX = elementStartX + currentMouseX - mouseStartX;
		var newElementY = elementStartY + currentMouseY - mouseStartY;

		// check bounds
		// note: the "-1" and "+1" is to avoid borders overlap
		if(newElementX < bounds[0])
			newElementX = bounds[0] + 1;
		if(newElementX + elementToMove.offsetWidth > bounds[2])
			newElementX = bounds[2] - elementToMove.offsetWidth - 1;
		if(newElementY < bounds[1])
			newElementY = bounds[1] + 1;
		if(newElementY + elementToMove.offsetHeight > bounds[3])
			newElementY = bounds[3] - elementToMove.offsetHeight - 1;
		
		// move element
		elementToMove.style.left = newElementX + 'px';
		elementToMove.style.top  = newElementY + 'px';

//		elementToMove.style.left = newElementX / elementToMove.parentNode.offsetWidth * 100 + '%';
//		elementToMove.style.top  = newElementY / elementToMove.parentNode.offsetHeight * 100 + '%';
	
		elementToMove.style.right = null;
		elementToMove.style.bottom = null;
		
		var i;
		for(i = 0; i < blocksToMove.length; i++)
		{
			blocksToMove[i].onMove();
		}
		return false;
	}
}

/**
 * finds the innermost draggable element starting from the one that generated the event "e"
 * (i.e.: the html element under mouse pointer), then setup the document's onmousemove function to
 * move the element around.
 */
function startDrag(e) 
{	
	var eventSource = nn6 ? e.target : event.srcElement;
	if(eventSource.tagName == 'HTML')
		return;

	while (eventSource != document.body && !hasClass(eventSource, "draggable"))
	{  	
		eventSource = nn6 ? eventSource.parentNode : eventSource.parentElement;
	}

	// if a draggable element was found, calculate its actual position
	if (hasClass(eventSource, "draggable"))
	{
		isdrag = true;
		elementToMove = eventSource;

		// set absolute positioning on the element		
		elementToMove.style.position = "absolute";
				
		// calculate start point
		elementStartX = elementToMove.offsetLeft;
		elementStartY = elementToMove.offsetTop;
		
		// calculate mouse start point
		mouseStartX = nn6 ? e.clientX : event.clientX;
		mouseStartY = nn6 ? e.clientY : event.clientY;
		
		// calculate bounds as left, top, width, height of the parent element
		if(getStyle(elementToMove.parentNode, "position") == 'absolute')
		{
			bounds[0] = 0;
			bounds[1] = 0;
		}
		else
		{
			bounds[0] = calculateOffsetLeft(elementToMove.parentNode);
			bounds[1] = calculateOffsetTop(elementToMove.parentNode);
		}
		bounds[2] = bounds[0] + elementToMove.parentNode.offsetWidth;
		bounds[3] = bounds[1] + elementToMove.parentNode.offsetHeight;		
		
		// either find the block related to the dragging element to call its onMove method
		blocksToMove = new Array();
		
		blocksToMoveScanner.scan(eventSource);
		document.onmousemove = movemouse;
		
		originalZIndex = getStyle(elementToMove, "z-index");
		elementToMove.style.zIndex = "3";
		
		return false;
	}
}

function stopDrag(e)
{
	isdrag=false; 
	if(elementToMove)
		elementToMove.style.zIndex=originalZIndex;
	elementToMove = null;
	document.onmousemove = null;
}

document.onmousedown = startDrag;
document.onmouseup = stopDrag;



/*************
 * Constants *
 *************/
var LEFT = 1;
var RIGHT = 2;
var UP = 4;
var DOWN = 8;
var HORIZONTAL = LEFT + RIGHT;
var VERTICAL = UP + DOWN;
var AUTO = HORIZONTAL + VERTICAL;

var START = 0;
var END = 1;
var SCROLLBARS_WIDTH = 18;

/**************
 * Inspectors *
 **************/

var inspectors = new Array();

/**
 * The canvas class.
 * This class is built on a div html element.
 */
function Canvas(htmlElement)
{
	/*
	 * initialization
	 */
	this.id = htmlElement.id;
	this.htmlElement = htmlElement;
	this.blocks = new Array();
	this.connectors = new Array();
	this.offsetLeft = calculateOffsetLeft(this.htmlElement);
	this.offsetTop = calculateOffsetTop(this.htmlElement);	
	
	this.width;
	this.height;

	// create the inner div element
	this.innerDiv = document.createElement("div");
	
	this.initCanvas = function()
	{
		// setup the inner div
		var children = this.htmlElement.childNodes;
		var i;
		var el;
		var n = children.length;
		for(i = 0; i < n; i++)
		{
			el = children[0];
			this.htmlElement.removeChild(el);
			this.innerDiv.appendChild(el);
			if(el.style)
				el.style.zIndex = "2";
		}
		this.htmlElement.appendChild(this.innerDiv);

		this.htmlElement.style.overflow = "auto";
		this.htmlElement.style.position = "relative";
		this.innerDiv.id = this.id + "_innerDiv";
		this.innerDiv.style.border = "none";
		this.innerDiv.style.padding = "0px";
		this.innerDiv.style.margin = "0px";
		this.innerDiv.style.position = "absolute";
		this.innerDiv.style.top = "0px";
		this.innerDiv.style.left = "0px";
		this.width = 0;
		this.height = 0;
		this.offsetLeft = calculateOffsetLeft(this.innerDiv);
		this.offsetTop = calculateOffsetTop(this.innerDiv);

		// inspect canvas children to identify first level blocks
		new DocumentScanner(this, true).scan(this.htmlElement);
		
		// now this.width and this.height are populated with minimum values needed for the inner
		// blocks to fit, add 2 to avoid border overlap;
		this.height += 2;
		this.width += 2;
		
		var visibleWidth = this.htmlElement.offsetWidth - 2; // - 2 is to avoid border overlap
		var visibleHeight = this.htmlElement.offsetHeight - 2; // - 2 is to avoid border overlap
		
		// consider the scrollbars width calculating the inner div size
		if(this.height > visibleHeight)
			visibleWidth -= SCROLLBARS_WIDTH;
		if(this.width > visibleWidth)
			visibleHeight -= SCROLLBARS_WIDTH;
			
		this.height = Math.max(this.height, visibleHeight);
		this.width = Math.max(this.width, visibleWidth);
		
		this.innerDiv.style.width = this.width + "px";
		this.innerDiv.style.height = this.height + "px";
		
		// init connectors
		for(i = 0; i < this.connectors.length; i++)
		{
			this.connectors[i].initConnector();
		}
	}
	
	this.visit = function(element)
	{
		if(element == this.htmlElement)
			return true;
	
		// check the element dimensions against the acutal size of the canvas
		this.width = Math.max(this.width, calculateOffsetLeft(element) - this.offsetLeft + element.offsetWidth);
		this.height = Math.max(this.height, calculateOffsetTop(element) - this.offsetTop + element.offsetHeight);
		
		if(isBlock(element))
		{
			// block found initialize it
			var newBlock = new Block(element, this);
			newBlock.initBlock();
			this.blocks.push(newBlock);
			return false;
		}
		else if(isConnector(element))
		{
			// connector found, just create it, source or destination blocks may not 
			// have been initialized yet
			var newConnector = new Connector(element, this);
			this.connectors.push(newConnector);
			return false;
		}
		else
		{
			// continue searching nested elements
			return true;
		}
	}
	
	/*
	 * methods
	 */	
	this.print = function()
	{
		var output = '<ul><legend>canvas: ' + this.id + '</legend>';
		var i;
		for(i = 0; i < this.blocks.length; i++)
		{
			output += '<li>';
			output += this.blocks[i].print();
			output += '</li>';
		}
		output += '</ul>';
		return output;
	}
	
	/*
	 * This function searches for a nested block with a given id
	 */
	this.findBlock = function(blockId)
	{
		var result;
		var i;
		for(i = 0; i < this.blocks.length && !result; i++)
		{
			result = this.blocks[i].findBlock(blockId);
		}
		
		return result;
	}
	
	this.toString = function()
	{
		return 'canvas: ' + this.id;		
	}
}

/*
 * Block class
 */
function Block(htmlElement, canvas)
{	
	/*
	 * initialization
	 */
	 
	this.canvas = canvas;
	this.htmlElement = htmlElement;
	this.id = htmlElement.id;
	this.blocks = new Array();
	this.moveListeners = new Array();
	
	if(this.id == 'description2_out1')
		var merda = 0;
	this.currentTop = calculateOffsetTop(this.htmlElement) - this.canvas.offsetTop;
	this.currentLeft = calculateOffsetLeft(this.htmlElement) - this.canvas.offsetLeft;
	
	this.visit = function(element)
	{
		if(element == this.htmlElement)
		{
			// exclude itself
			return true;
		}

		if(isBlock(element))
		{
			var innerBlock = new Block(element, this.canvas);
			innerBlock.initBlock();
			this.blocks.push(innerBlock);
			this.moveListeners.push(innerBlock);
			return false;
		}
		else
			return true;
	}
	
	this.initBlock = function()
	{
		// inspect block children to identify nested blocks
		
		new DocumentScanner(this, true).scan(this.htmlElement);
	}
	
	this.top = function()
	{
		return this.currentTop;
	}
	
	this.left = function()
	{
		return this.currentLeft;
	}
	
	this.width = function()
	{
		return this.htmlElement.offsetWidth;		
	}
	
	this.height = function()
	{
		return this.htmlElement.offsetHeight;
	}
	
	/*
	 * methods
	 */	
	this.print = function()
	{
		var output = 'block: ' + this.id;
		if(this.blocks.length > 0)
		{
			output += '<ul>';
			var i;
			for(i = 0; i < this.blocks.length; i++)
			{
				output += '<li>';
				output += this.blocks[i].print();
				output += '</li>';
			}
			output += '</ul>';
		}
		return output;
	}
	
	/*
	 * This function searches for a nested block (or the block itself) with a given id
	 */
	this.findBlock = function(blockId)
	{
		if(this.id == blockId)
			return this;
			
		var result;
		var i;
		for(i = 0; i < this.blocks.length && !result; i++)
		{
			result = this.blocks[i].findBlock(blockId);
		}
		
		return result;
	}
	
	this.move = function(left, top)
	{
		this.htmlElement.style.left = left;
		this.htmlElement.style.top = top;
		
		this.onMove();
	}
		
	this.onMove = function()
	{
		var i;
		this.currentLeft = calculateOffsetLeft(this.htmlElement) - this.canvas.offsetLeft;
		this.currentTop = calculateOffsetTop(this.htmlElement) - this.canvas.offsetTop;
		// notify listeners
		for(i = 0; i < this.moveListeners.length; i++)
		{
			this.moveListeners[i].onMove();
		}
	}
	
	this.toString = function()
	{
		return 'block: ' + this.id;
	}
}

/**
 * This class represents a connector segment, it is drawn via a div element.
 * A segment has a starting point defined by the properties startX and startY, a length,
 * a thickness and an orientation.
 * Allowed values for the orientation property are defined by the constants UP, LEFT, DOWN and RIGHT.
 */
function Segment(id, parentElement)
{
	this.id = id;
	this.htmlElement = document.createElement('div');
	this.htmlElement.id = id;
	this.htmlElement.style.position = 'absolute';
	this.htmlElement.style.overflow = 'hidden';
	parentElement.appendChild(this.htmlElement);

	this.startX;
	this.startY;
	this.length;
	this.thickness;
	this.orientation;
	this.nextSegment;
	this.visible = true;
	
	/**
	 * draw the segment. This operation is cascaded to next segment if any.
	 */
	this.draw = function()
	{
		// set properties to next segment
		if(this.nextSegment)
		{
			this.nextSegment.startX = this.getEndX();
			this.nextSegment.startY = this.getEndY();			
		}
		
		if(this.visible)
			this.htmlElement.style.display = 'block';
		else
			this.htmlElement.style.display = 'none';
	
		switch(this.orientation)
		{
			case LEFT:
				this.htmlElement.style.left = (this.startX - this.length) + "px";				
				this.htmlElement.style.top = this.startY + "px";
				this.htmlElement.style.width = this.length + "px";
				this.htmlElement.style.height = this.thickness + "px";
				break;
			case RIGHT:
				this.htmlElement.style.left = this.startX + "px";
				this.htmlElement.style.top = this.startY + "px";
				if(this.nextSegment)
					this.htmlElement.style.width = this.length + this.thickness + "px";
				else
					this.htmlElement.style.width = this.length + "px";
				this.htmlElement.style.height = this.thickness + "px";
				break;
			case UP:
				this.htmlElement.style.left = this.startX + "px";
				this.htmlElement.style.top = (this.startY - this.length) + "px";
				this.htmlElement.style.width = this.thickness + "px";
				this.htmlElement.style.height = this.length + "px";
				break;
			case DOWN:
				this.htmlElement.style.left = this.startX + "px";
				this.htmlElement.style.top = this.startY + "px";
				this.htmlElement.style.width = this.thickness + "px";
				if(this.nextSegment)
					this.htmlElement.style.height = this.length + this.thickness + "px";
				else
					this.htmlElement.style.height = this.length + "px";
				break;
		}
		
		if(this.nextSegment)
			this.nextSegment.draw();
	}
	
	/**
	 * Returns the "left" coordinate of the end point of this segment
	 */
	this.getEndX = function()
	{		
		switch(this.orientation)
		{
			case LEFT: return this.startX - this.length;
			case RIGHT: return this.startX + this.length;
			case DOWN: return this.startX;
			case UP: return this.startX;
		}
	}
	
	/**
	 * Returns the "top" coordinate of the end point of this segment
	 */
	this.getEndY = function()
	{		
		switch(this.orientation)
		{
			case LEFT: return this.startY;
			case RIGHT: return this.startY;
			case DOWN: return this.startY + this.length;
			case UP: return this.startY - this.length;
		}
	}
		
	/**
	 * Append another segment to the end point of this.
	 * If another segment is already appended to this, cascades the operation so
	 * the given next segment will be appended to the tail of the segments chain.
	 */
	this.append = function(nextSegment)
	{
		if(!nextSegment)
			return;
		if(!this.nextSegment)
		{
			this.nextSegment = nextSegment;
			this.nextSegment.startX = this.getEndX();
			this.nextSegment.startY = this.getEndY();
		}
		else
			this.nextSegment.append(nextSegment);
	}
	
	this.detach = function()
	{
		var s = this.nextSegment;
		this.nextSegment = null;
		return s;
	}
	
	/**
	 * hides this segment and all the following
	 */
	this.cascadeHide = function()
	{
		this.visible = false;
		if(this.nextSegment)
			this.nextSegment.cascadeHide();
	}
}
/**
 * Connector class.
 * The init function takes two Block objects as arguments representing 
 * the source and destination of the connector
 */
function Connector(htmlElement, canvas)
{
	/**
	 * declaring html element
	 */
	this.htmlElement = htmlElement;
	
	/**
	 * the canvas this connector is in
	 */
	this.canvas = canvas;
	
	/**
	 * the source block
	 */
	this.source = null;
	
	/**
	 * the destination block
	 */
	this.destination = null;	
	
	/**
	 * preferred orientation
	 */
	this.preferredSourceOrientation = AUTO;
	this.preferredDestinationOrientation = AUTO;
	
	/**
	 * css class to be applied to the connector's segments
	 */
	this.connectorClass;
	
	/**
	 * minimum length for a connector segment.
	 */
	this.minSegmentLength = 10;

	/**
	 * size of the connector, i.e.: thickness of the segments.
	 */
	this.size = 1;
	
	/**
	 * connector's color
	 */
	this.color = 'black';
	
	/**
	 * move listeners, they are notify when connector moves
	 */
	this.moveListeners = new Array();
	
	this.firstSegment;
	
	this.segmentsPool;
	
	this.segmentsNumber = 0;
	
	this.strategy;
		
	this.initConnector = function()
	{
		// detect the connector id
		if(this.htmlElement.id)
			this.id = this.htmlElement.id;
		else
			this.id = this.htmlElement.className;
			
		// split the class name to get the ids of the source and destination blocks
		var splitted = htmlElement.className.split(' ');
		if(splitted.length < 3)
		{
			alert('Unable to create connector \'' + id + '\', class is not in the correct format: connector <sourceBlockId>, <destBlockId>');
			return;
		}
		
		this.connectorClass = splitted[0] + ' ' + splitted[1] + ' ' + splitted[2];
		
		this.source = this.canvas.findBlock(splitted[1]);
		if(!this.source)
		{
			alert('cannot find source block with id \'' + splitted[1] + '\'');
			return;
		}
		
		this.destination = this.canvas.findBlock(splitted[2]);
		if(!this.destination)
		{
			alert('cannot find destination block with id \'' + splitted[2] + '\'');
			return;
		}
		
		// check preferred orientation
		if(hasClass(this.htmlElement, 'vertical'))
		{
			this.preferredSourceOrientation = VERTICAL;
			this.preferredDestinationOrientation = VERTICAL;
		}
		else if(hasClass(this.htmlElement, 'horizontal'))
		{
			this.preferredSourceOrientation = HORIZONTAL;
			this.preferredDestinationOrientation = HORIZONTAL;
		}
		else
		{
			// check preferred orientation on source side
			if(hasClass(this.htmlElement, 'vertical_start'))
				this.preferredSourceOrientation = VERTICAL;
			else if(hasClass(this.htmlElement, 'horizontal_start'))
				this.preferredSourceOrientation = HORIZONTAL;
			else if(hasClass(this.htmlElement, 'left_start'))
				this.preferredSourceOrientation = LEFT;
			else if(hasClass(this.htmlElement, 'right_start'))
				this.preferredSourceOrientation = RIGHT;
			else if(hasClass(this.htmlElement, 'up_start'))
				this.preferredSourceOrientation = UP;
			else if(hasClass(this.htmlElement, 'down_start'))
				this.preferredSourceOrientation = DOWN;
			
			// check preferred orientation on destination side
			if(hasClass(this.htmlElement, 'vertical_end'))
				this.preferredDestinationOrientation = VERTICAL;
			else if(hasClass(this.htmlElement, 'horizontal_end'))
				this.preferredDestinationOrientation = HORIZONTAL;
			else if(hasClass(this.htmlElement, 'left_end'))
				this.preferredDestinationOrientation = LEFT;
			else if(hasClass(this.htmlElement, 'right_end'))
				this.preferredDestinationOrientation = RIGHT;
			else if(hasClass(this.htmlElement, 'up_end'))
				this.preferredDestinationOrientation = UP;
			else if(hasClass(this.htmlElement, 'down_end'))
				this.preferredDestinationOrientation = DOWN;
		}
		
		// get the first strategy as default
		this.strategy = strategies[0](this);
		this.repaint();
		
		this.source.moveListeners.push(this);
		this.destination.moveListeners.push(this);
		
		// call inspectors for this connector
		var i;
		for(i = 0; i < inspectors.length; i++)
		{
			inspectors[i].inspect(this);
		}
		
		// remove old html element
		this.htmlElement.parentNode.removeChild(this.htmlElement);
	}
	
	this.getStartSegment = function()
	{
		return this.firstSegment;
	}
	
	this.getEndSegment = function()
	{
		var s = this.firstSegment;
		while(s.nextSegment)
			s = s.nextSegment;
		return s;
	}
	
	this.getMiddleSegment = function()
	{
		if(!this.strategy)
			return null;
		else
			return this.strategy.getMiddleSegment();
	}
	
	this.createSegment = function()
	{
		var segment;
		
		// if the pool contains more objects, borrow the segment, create it otherwise
		if(this.segmentsPool)
		{
			segment = this.segmentsPool;
			this.segmentsPool = this.segmentsPool.detach();
		}
		else
		{		
			segment = new Segment(this.id + "_" + (this.segmentsNumber + 1), this.canvas.htmlElement);
			segment.htmlElement.className = this.connectorClass;
			if(!getStyle(segment.htmlElement, 'background-color'))
				segment.htmlElement.style.backgroundColor = this.color;
			segment.thickness = this.size;
		}
		this.segmentsNumber++;
		
		if(this.firstSegment)
			this.firstSegment.append(segment);
		else
			this.firstSegment = segment;
		segment.visible = true;
		return segment;
	}
	
	/**
	 * Repaints the connector
	 */
	this.repaint = function()
	{
		// check strategies fitness and choose the best fitting one
		var i;
		var maxFitness = 0;
		var fitness;
		var s;
		
		// check if any strategy is possible with preferredOrientation
		for(i = 0; i < strategies.length; i++)
		{
			this.clearSegments();
			
			fitness = 0;
			s = strategies[i](this);
			if(s.isApplicable())
			{
				fitness++;
				s.paint();
				// check resulting orientation against the preferred orientations
				if((this.firstSegment.orientation & this.preferredSourceOrientation) != 0)
					fitness++;
				if((this.getEndSegment().orientation & this.preferredDestinationOrientation) != 0)
					fitness++;
			}
			
			if(fitness > maxFitness)
			{
				this.strategy = s;
				maxFitness = fitness;
			}
		}			
		
		this.clearSegments();

		this.strategy.paint();
		this.firstSegment.draw();

		// this is needed to actually hide unused html elements	
		if(this.segmentsPool)
			this.segmentsPool.draw();
	}
	
	/**
	 * Hide all the segments and return them to pool
	 */
	this.clearSegments = function()
	{
		if(this.firstSegment)
		{
			this.firstSegment.cascadeHide();
			this.firstSegment.append(this.segmentsPool);
			this.segmentsPool = this.firstSegment;
			this.firstSegment = null;
		}	
	}
		
	this.onMove = function()
	{
		this.repaint();
		
		// notify listeners
		var i;
		for(i = 0; i < this.moveListeners.length; i++)
			this.moveListeners[i].onMove();
	}
}

var strategies = new Array();

function ConnectorEnd(htmlElement, connector, side)
{
	this.side = side;
	this.htmlElement = htmlElement;
	this.connector = connector;
	connector.canvas.htmlElement.appendChild(htmlElement);
	// strip extension
	if(this.htmlElement.tagName.toLowerCase() == "img")
	{
		this.src = this.htmlElement.src.substring(0, this.htmlElement.src.lastIndexOf('.'));
		this.srcExtension = this.htmlElement.src.substring(this.htmlElement.src.lastIndexOf('.'));
		this.htmlElement.style.zIndex = getStyle(this.connector.htmlElement, "z-index");
	}
	
	this.orientation;
	
	this.repaint = function()
	{
		this.htmlElement.style.position = 'absolute';
				
		var left;
		var top;
		var segment;
		var orientation;
		
		if(this.side == START)
		{
			segment = connector.getStartSegment();
			left = segment.startX;
			top = segment.startY;
			orientation = segment.orientation;
			// swap orientation
			if((orientation & VERTICAL) != 0)
				orientation = (~orientation) & VERTICAL;
			else
				orientation = (~orientation) & HORIZONTAL;
		}
		else
		{
			segment = connector.getEndSegment();
			left = segment.getEndX();
			top = segment.getEndY();
			orientation = segment.orientation;
		}
		
		switch(orientation)
		{
			case LEFT:
				top -= (this.htmlElement.offsetHeight - segment.thickness) / 2;
				break;
			case RIGHT:
				left -= this.htmlElement.offsetWidth;
				top -= (this.htmlElement.offsetHeight - segment.thickness) / 2;
				break;
			case DOWN:
				top -= this.htmlElement.offsetHeight;
				left -= (this.htmlElement.offsetWidth - segment.thickness) / 2;
				break;
			case UP:
				left -= (this.htmlElement.offsetWidth - segment.thickness) / 2;
				break;
		}
		
		this.htmlElement.style.left = Math.ceil(left) + "px";
		this.htmlElement.style.top = Math.ceil(top) + "px";
		
		if(this.htmlElement.tagName.toLowerCase() == "img" && this.orientation != orientation)
		{
			var orientationSuffix;
			switch(orientation)
			{
				case UP: orientationSuffix = "u"; break;
				case DOWN: orientationSuffix = "d"; break;
				case LEFT: orientationSuffix = "l"; break;
				case RIGHT: orientationSuffix = "r"; break;
			}
			this.htmlElement.src = this.src + "_" + orientationSuffix + this.srcExtension;
		}
		this.orientation = orientation;
	}
	
	this.onMove = function()
	{
		this.repaint();
	}
}

function SideConnectorLabel(connector, htmlElement, side)
{
	this.connector = connector;
	this.htmlElement = htmlElement;
	this.side = side;
	this.connector.htmlElement.parentNode.appendChild(htmlElement);
		
	this.repaint = function()
	{
		this.htmlElement.style.position = 'absolute';
		var left;
		var top;
		var segment;

		if(this.side == START)
		{	
			segment = this.connector.getStartSegment();
			left = segment.startX;
			top = segment.startY;
			if(segment.orientation == LEFT)
				left -= this.htmlElement.offsetWidth;
			if(segment.orientation == UP)
				top -= this.htmlElement.offsetHeight;
				
			if((segment.orientation & HORIZONTAL) != 0 && top < this.connector.getEndSegment().getEndY())
				top -= this.htmlElement.offsetHeight;
			if((segment.orientation & VERTICAL) != 0 && left < this.connector.getEndSegment().getEndX())
				left -= this.htmlElement.offsetWidth;
		}
		else
		{	
			segment = this.connector.getEndSegment();
			left = segment.getEndX();
			top = segment.getEndY();
			if(segment.orientation == RIGHT)
				left -= this.htmlElement.offsetWidth;
			if(segment.orientation == DOWN)
				top -= this.htmlElement.offsetHeight;
			if((segment.orientation & HORIZONTAL) != 0 && top < this.connector.getStartSegment().startY)
				top -= this.htmlElement.offsetHeight;
			if((segment.orientation & VERTICAL) != 0 && left < this.connector.getStartSegment().startX)
				left -= this.htmlElement.offsetWidth;
		}
		
		this.htmlElement.style.left = Math.ceil(left) + "px";
		this.htmlElement.style.top = Math.ceil(top) + "px";
	}
	
	this.onMove = function()
	{
		this.repaint();
	}
}

function MiddleConnectorLabel(connector, htmlElement)
{
	this.connector = connector;
	this.htmlElement = htmlElement;
	this.connector.canvas.htmlElement.appendChild(htmlElement);
	
	this.repaint = function()
	{
		this.htmlElement.style.position = 'absolute';
		
		var left;
		var top;
		var segment = connector.getMiddleSegment();

		if((segment.orientation & VERTICAL) != 0)
		{
			// put label at middle height on right side of the connector
			top = segment.htmlElement.offsetTop + (segment.htmlElement.offsetHeight - this.htmlElement.offsetHeight) / 2;
			left = segment.htmlElement.offsetLeft;
		}
		else
		{
			// put connector below the connector at middle widths
			top = segment.htmlElement.offsetTop;
			left = segment.htmlElement.offsetLeft + (segment.htmlElement.offsetWidth - this.htmlElement.offsetWidth) / 2;;
		}
		
		this.htmlElement.style.left = Math.ceil(left) + "px";
		this.htmlElement.style.top = Math.ceil(top) + "px";
	}
	
	this.onMove = function()
	{
		this.repaint();
	}
}

/*
 * Inspector classes
 */

function ConnectorEndsInspector()
{
	this.inspect = function(connector)
	{
		var children = connector.htmlElement.childNodes;
		var i;
		for(i = 0; i < children.length; i++)
		{
			if(hasClass(children[i], "connector-end"))
			{
				var newElement = new ConnectorEnd(children[i], connector, END);
				newElement.repaint();
				connector.moveListeners.push(newElement);
			}
			else if(hasClass(children[i], "connector-start"))
			{
				var newElement = new ConnectorEnd(children[i], connector, START);
				newElement.repaint();
				connector.moveListeners.push(newElement);
			}
		}
	}
}

function ConnectorLabelsInspector()
{
	this.inspect = function(connector)
	{
		var children = connector.htmlElement.childNodes;
		var i;
		for(i = 0; i < children.length; i++)
		{
			if(hasClass(children[i], "source-label"))
			{
				var newElement = new SideConnectorLabel(connector, children[i], START);
				newElement.repaint();
				connector.moveListeners.push(newElement);
			}
			else if(hasClass(children[i], "middle-label"))
			{
				var newElement = new MiddleConnectorLabel(connector, children[i]);
				newElement.repaint();
				connector.moveListeners.push(newElement);
			}
			else if(hasClass(children[i], "destination-label"))
			{
				var newElement = new SideConnectorLabel(connector, children[i], END);
				newElement.repaint();
				connector.moveListeners.push(newElement);
			}
		}
	}
}

/*
 * Inspector registration
 */

inspectors.push(new ConnectorEndsInspector());
inspectors.push(new ConnectorLabelsInspector());

/*
 * an array containing all the canvases in document
 */
var canvases = new Array();

/*
 * This function initializes the js_graph objects inspecting the html document
 */
function initPageObjects()
{
	canvases = new Array();

	if(isCanvas(document.body))
	{
		var newCanvas = new Canvas(document.body);
		newCanvas.initCanvas();
		canvases.push(newCanvas);
	}
	else
	{	
		var divs = document.getElementsByTagName('div');
		var i;
		for(i = 0; i < divs.length; i++)
		{
			if(isCanvas(divs[i]) && !findCanvas(divs[i].id))
			{
				var newCanvas = new Canvas(divs[i]);
				newCanvas.initCanvas();
				canvases.push(newCanvas);

                                // trigger a move on each block to repaint it
                                for (var i = 0; i < newCanvas.blocks.length; i++) {
                                  newCanvas.blocks[i].onMove();
                                }
			}
		}
	}
}


/*
 * Utility functions
 */


function findCanvas(canvasId)
{	
	var i;
	for(i = 0; i < canvases.length; i++)
		if(canvases[i].id == canvasId)
			return canvases[i];
	return null;
}

function findBlock(blockId)
{
	var i;
	for(i = 0; i < canvases.length; i++)
	{
		var block = canvases[i].findBlock(blockId);
		if(block)
			return block;
	}
	return null;
}
 
/*
 * This function determines whether a html element is to be considered a canvas
 */
function isBlock(htmlElement)
{
	return hasClass(htmlElement, 'block');
}

/*
 * This function determines whether a html element is to be considered a block
 */
function isCanvas(htmlElement)
{
	return hasClass(htmlElement, 'canvas');
}

/*
 * This function determines whether a html element is to be considered a connector
 */
function isConnector(htmlElement)
{
	return hasClass(htmlElement, 'connector') || hasClass(htmlElement, 'connector_feedback');
	// return htmlElement.className && htmlElement.className.match(new RegExp('connector .*'));
}

/*
 * This function calculates the absolute 'top' value for a html node
 */
function calculateOffsetTop(obj)
{
	var curtop = 0;
	if (obj.offsetParent)
	{
		curtop = obj.offsetTop
		while (obj = obj.offsetParent) 
			curtop += obj.offsetTop
	}
	else if (obj.y)
		curtop += obj.y;
	return curtop;
}

/*
 * This function calculates the absolute 'left' value for a html node
 */
function calculateOffsetLeft(obj)
{
	var curleft = 0;
	if (obj.offsetParent)
	{
		curleft = obj.offsetLeft
		while (obj = obj.offsetParent) 
		{
			curleft += obj.offsetLeft;
		}
	}
	else if (obj.x)
		curleft += obj.x;
	return curleft;
}

function parseBorder(obj, side)
{
	var sizeString = getStyle(obj, "border-" + side + "-width");
	if(sizeString && sizeString != "")
	{
		if(sizeString.substring(sizeString.length - 2) == "px")
			return parseInt(sizeString.substring(0, sizeString.length - 2));
	}
	return 0;
}

function hasClass(element, className)
{
	if(!element || !element.className)
		return false;
		
	var classes = element.className.split(' ');
	var i;
	for(i = 0; i < classes.length; i++)
		if(classes[i] == className)
			return true;
	return false;
}

/**
 * This function retrieves the actual value of a style property even if it is set via css.
 */
function getStyle(node, styleProp)
{
	// if not an element
	if( node.nodeType != 1)
		return;
		
	var value;
	if (node.currentStyle)
	{
		// ie case
		styleProp = replaceDashWithCamelNotation(styleProp);
		value = node.currentStyle[styleProp];
	}
	else if (window.getComputedStyle)
	{
		// mozilla case
		value = document.defaultView.getComputedStyle(node, null).getPropertyValue(styleProp);
	}
	
	return value;
}

function replaceDashWithCamelNotation(value)
{
	var pos = value.indexOf('-');
	while(pos > 0 && value.length > pos + 1)
	{
		value = value.substring(0, pos) + value.substring(pos + 1, pos + 2).toUpperCase() + value.substring(pos + 2);
		pos = value.indexOf('-');
	}
	return value;
}


/*******************************
 * Connector paint strategies. *
 *******************************/
 
/**
 * Horizontal "S" routing strategy.
 */
function HorizontalSStrategy(connector)
{
	this.connector = connector;
	
	this.startSegment;
	this.middleSegment;
	this.endSegment;
	
	this.strategyName = "horizontal_s";
	
	this.getMiddleSegment = function()
	{
		return this.middleSegment;
	}
	
	this.isApplicable = function()
	{
		var sourceLeft = this.connector.source.left();
		var sourceWidth = this.connector.source.width();
		var destinationLeft = this.connector.destination.left();
		var destinationWidth = this.connector.destination.width();
		
		return Math.abs(2 * destinationLeft + destinationWidth - (2 * sourceLeft + sourceWidth)) - (sourceWidth + destinationWidth) > 4 * this.connector.minSegmentLength;
	}
	
	this.paint = function()
	{
		this.startSegment = connector.createSegment();
		this.middleSegment = connector.createSegment();
		this.endSegment = connector.createSegment();
		
		var sourceLeft = this.connector.source.left();
		var sourceTop = this.connector.source.top();
		var sourceWidth = this.connector.source.width();
		var sourceHeight = this.connector.source.height();
		
		var destinationLeft = this.connector.destination.left();
		var destinationTop = this.connector.destination.top();
		var destinationWidth = this.connector.destination.width();
		var destinationHeight = this.connector.destination.height();
		
		var hLength;
		
		this.startSegment.startY = Math.floor(sourceTop + sourceHeight / 2);
			
		// deduce which face to use on source and destination blocks
		if(sourceLeft + sourceWidth / 2 < destinationLeft + destinationWidth / 2)
		{
			// use left side of the source block and right side of the destination block
			this.startSegment.startX = sourceLeft + sourceWidth;
			hLength = destinationLeft - (sourceLeft + sourceWidth);
		}
		else
		{
			// use right side of the source block and left side of the destination block
			this.startSegment.startX = sourceLeft;
			hLength = destinationLeft + destinationWidth - sourceLeft;
		}

		// first horizontal segment positioning
		this.startSegment.length = Math.floor(Math.abs(hLength) / 2);
		this.startSegment.orientation = hLength > 0 ? RIGHT : LEFT;
		
		// vertical segment positioning			
		var vLength = Math.floor(destinationTop + destinationHeight / 2 - (sourceTop + sourceHeight / 2));		
		this.middleSegment.length = Math.abs(vLength);
//		if(vLength == 0)
//			this.middleSegment.visible = false;
		this.middleSegment.orientation = vLength > 0 ? DOWN : UP;
		
		// second horizontal segment positioning
		this.endSegment.length = Math.floor(Math.abs(hLength) / 2);
		this.endSegment.orientation = hLength > 0 ? RIGHT : LEFT;
	}
}

/**
 * Vertical "S" routing strategy.
 */
function VerticalSStrategy(connector)
{
	this.connector = connector;
	
	this.startSegment;
	this.middleSegment;
	this.endSegment;
	
	this.strategyName = "vertical_s";
	
	this.getMiddleSegment = function()
	{
		return this.middleSegment;
	}	
	
	this.isApplicable = function()
	{
		var sourceTop = this.connector.source.top();
		var sourceHeight = this.connector.source.height();
		var destinationTop = this.connector.destination.top();
		var destinationHeight = this.connector.destination.height();
		return Math.abs(2 * destinationTop + destinationHeight - (2 * sourceTop + sourceHeight)) - (sourceHeight + destinationHeight) > 4 * this.connector.minSegmentLength;
	}
	
	this.paint = function()
	{
		this.startSegment = connector.createSegment();
		this.middleSegment = connector.createSegment();
		this.endSegment = connector.createSegment();
		
		var sourceLeft = this.connector.source.left();
		var sourceTop = this.connector.source.top();
		var sourceWidth = this.connector.source.width();
		var sourceHeight = this.connector.source.height();
		
		var destinationLeft = this.connector.destination.left();
		var destinationTop = this.connector.destination.top();
		var destinationWidth = this.connector.destination.width();
		var destinationHeight = this.connector.destination.height();
		
		var vLength;
		
		this.startSegment.startX = 	Math.floor(sourceLeft + sourceWidth / 2);
			
		// deduce which face to use on source and destination blocks
		if(sourceTop + sourceHeight / 2 < destinationTop + destinationHeight / 2)
		{
			// use bottom side of the source block and top side of destination block
			this.startSegment.startY = sourceTop + sourceHeight;
			vLength = destinationTop - (sourceTop + sourceHeight);
		}
		else
		{
			// use top side of the source block and bottom side of the destination block
			this.startSegment.startY = sourceTop;
			vLength = destinationTop + destinationHeight - sourceTop;
		}
		
		// first vertical segment positioning
		this.startSegment.length = Math.floor(Math.abs(vLength) / 2);
		this.startSegment.orientation = vLength > 0 ? DOWN : UP;
		
		// horizontal segment positioning
		var hLength = Math.floor(destinationLeft + destinationWidth / 2 - (sourceLeft + sourceWidth / 2));
		this.middleSegment.length = Math.abs(hLength);
		this.middleSegment.orientation = hLength > 0 ? RIGHT : LEFT;
					
		// second vertical segment positioning
		this.endSegment.length = Math.floor(Math.abs(vLength) / 2);
		this.endSegment.orientation = vLength > 0 ? DOWN : UP;
	}
}

/**
 * A horizontal "L" connector routing strategy
 */
function HorizontalLStrategy(connector)
{
	this.connector = connector;
	
	this.destination;
	
	this.startSegment;
	this.endSegment;
	
	this.strategyName = "horizontal_L";
	
	this.isApplicable = function()
	{
		var destMiddle = Math.floor(this.connector.destination.left() + this.connector.destination.width() / 2);
		var sl = this.connector.source.left();
		var sw = this.connector.source.width();
		var dt = this.connector.destination.top();
		var dh = this.connector.destination.height();
		var sourceMiddle = Math.floor(this.connector.source.top() + this.connector.source.height() / 2);

		if(destMiddle > sl && destMiddle < sl + sw)
			return false;
		if(sourceMiddle > dt && sourceMiddle < dt + dh)
			return false;
		return true;
	}
	
	/**
	 * Chooses the longest segment as the "middle" segment.
	 */
	this.getMiddleSegment = function()
	{
		if(this.startSegment.length > this.endSegment.length)
			return this.startSegment;
		else
			return this.endSegment;
	}
	
	this.paint = function()
	{
		this.startSegment = this.connector.createSegment();
		this.endSegment = this.connector.createSegment();
		
		var destMiddleX = Math.floor(this.connector.destination.left() + this.connector.destination.width() / 2);
		var sl = this.connector.source.left();
		var sw = this.connector.source.width();
		var dt = this.connector.destination.top();
		var dh = this.connector.destination.height();
		
		this.startSegment.startY = Math.floor(this.connector.source.top() + this.connector.source.height() / 2);
		
		// decide which side of the source block to connect to
		if(Math.abs(destMiddleX - sl) < Math.abs(destMiddleX - (sl + sw)))
		{
			// use the left face
			this.startSegment.orientation = (destMiddleX < sl) ? LEFT : RIGHT;				
			this.startSegment.startX = sl;
		}
		else
		{
			// use the right face
			this.startSegment.orientation = (destMiddleX > (sl + sw)) ? RIGHT : LEFT;
			this.startSegment.startX = sl + sw;
		}
		
		this.startSegment.length = Math.abs(destMiddleX - this.startSegment.startX);
		
		// decide which side of the destination block to connect to
		if(Math.abs(this.startSegment.startY - dt) < Math.abs(this.startSegment.startY - (dt + dh)))
		{
			// use the upper face
			this.endSegment.orientation = (this.startSegment.startY < dt) ? DOWN : UP;
			this.endSegment.length = Math.abs(this.startSegment.startY - dt);
		}
		else
		{
			// use the lower face
			this.endSegment.orientation = (this.startSegment.startY > (dt + dh)) ? UP : DOWN;
			this.endSegment.length = Math.abs(this.startSegment.startY - (dt + dh));
		}
	}
}

/**
 * Vertical "L" connector routing strategy
 */
function VerticalLStrategy(connector)
{
	this.connector = connector;
	
	this.startSegment;
	this.endSegment;
	
	this.strategyName = "vertical_L";
	
	this.isApplicable = function()
	{
		var sourceMiddle = Math.floor(this.connector.source.left() + this.connector.source.width() / 2);
		var dl = this.connector.destination.left();
		var dw = this.connector.destination.width();
		var st = this.connector.source.top();
		var sh = this.connector.source.height();
		var destMiddle = Math.floor(this.connector.destination.top() + this.connector.destination.height() / 2);

		if(sourceMiddle > dl && sourceMiddle < dl + dw)
			return false;
		if(destMiddle > st && destMiddle < st + sh)
			return false;
		return true;	
	}
	
	/**
	 * Chooses the longest segment as the "middle" segment.
	 */
	this.getMiddleSegment = function()
	{
		if(this.startSegment.length > this.endSegment.length)
			return this.startSegment;
		else
			return this.endSegment;
	}
	
	this.paint = function()
	{
		this.startSegment = this.connector.createSegment();
		this.endSegment = this.connector.createSegment();
		
		var destMiddleY = Math.floor(this.connector.destination.top() + this.connector.destination.height() / 2);
		var dl = this.connector.destination.left();
		var dw = this.connector.destination.width();
		var st = this.connector.source.top();
		var sh = this.connector.source.height();
		
		this.startSegment.startX = Math.floor(this.connector.source.left() + this.connector.source.width() / 2);
		
		// decide which side of the source block to connect to
		if(Math.abs(destMiddleY - st) < Math.abs(destMiddleY - (st + sh)))
		{
			// use the upper face
			this.startSegment.orientation = (destMiddleY < st) ? UP : DOWN;
			this.startSegment.startY = st;
		}
		else
		{
			// use the lower face
			this.startSegment.orientation = (destMiddleY > (st + sh)) ? DOWN : UP;
			this.startSegment.startY = st + sh;
		}
		
		this.startSegment.length = Math.abs(destMiddleY - this.startSegment.startY);
		
		// decide which side of the destination block to connect to
		if(Math.abs(this.startSegment.startX - dl) < Math.abs(this.startSegment.startX - (dl + dw)))
		{
			// use the left face
			this.endSegment.orientation = (this.startSegment.startX < dl) ? RIGHT : LEFT;
			this.endSegment.length = Math.abs(this.startSegment.startX - dl);
		}
		else
		{
			// use the right face
			this.endSegment.orientation = (this.startSegment.startX > dl + dw) ? LEFT : RIGHT;
			this.endSegment.length = Math.abs(this.startSegment.startX - (dl + dw));
		}
	}
}

function HorizontalCStrategy(connector, startOrientation)
{
	this.connector = connector;
	
	this.startSegment;
	this.middleSegment;
	this.endSegment;
	
	this.strategyName = "horizontal_c";
	
	this.getMiddleSegment = function()
	{
		return this.middleSegment;
	}	
	
	this.isApplicable = function()
	{
		return true;
	}
	
	this.paint = function()
	{
		this.startSegment = connector.createSegment();
		this.middleSegment = connector.createSegment();
		this.endSegment = connector.createSegment();
		
		var sign = 1;
		if(startOrientation == RIGHT)
			sign = -1;
		
		var startX = this.connector.source.left();
		if(startOrientation == RIGHT)
			startX += this.connector.source.width();
		var startY = Math.floor(this.connector.source.top() + this.connector.source.height() / 2);
		
		var endX = this.connector.destination.left();
		if(startOrientation == RIGHT)
			endX += this.connector.destination.width();
		var endY = Math.floor(this.connector.destination.top() + this.connector.destination.height() / 2);

		this.startSegment.startX = startX;
		this.startSegment.startY = startY;
		this.startSegment.orientation = startOrientation;
		this.startSegment.length = this.connector.minSegmentLength + Math.max(0, sign * (startX - endX));
		
		var vLength = endY - startY;
		this.middleSegment.orientation = vLength > 0 ? DOWN : UP;
		this.middleSegment.length = Math.abs(vLength);
		
		this.endSegment.orientation = startOrientation == LEFT ? RIGHT : LEFT;
		this.endSegment.length = Math.max(0, sign * (endX - startX)) + this.connector.minSegmentLength;
	}
}

function VerticalCStrategy(connector, startOrientation)
{
	this.connector = connector;
	
	this.startSegment;
	this.middleSegment;
	this.endSegment;
	
	this.strategyName = "vertical_c";
	
	this.getMiddleSegment = function()
	{
		return this.middleSegment;
	}	
	
	this.isApplicable = function()
	{
		return true;
	}
	
	this.paint = function()
	{
		this.startSegment = connector.createSegment();
		this.middleSegment = connector.createSegment();
		this.endSegment = connector.createSegment();
		
		var sign = 1;
		if(startOrientation == DOWN)
			sign = -1;
		
		var startY = this.connector.source.top();
		if(startOrientation == DOWN)
			startY += this.connector.source.height();
		var startX = Math.floor(this.connector.source.left() + this.connector.source.width() / 2);
		
		var endY = this.connector.destination.top();
		if(startOrientation == DOWN)
			endY += this.connector.destination.height();
		var endX = Math.floor(this.connector.destination.left() + this.connector.destination.width() / 2);

		this.startSegment.startX = startX;
		this.startSegment.startY = startY;
		this.startSegment.orientation = startOrientation;
		this.startSegment.length = this.connector.minSegmentLength + Math.max(0, sign * (startY - endY));
		
		var hLength = endX - startX;
		this.middleSegment.orientation = hLength > 0 ? RIGHT : LEFT;
		this.middleSegment.length = Math.abs(hLength);
		
		this.endSegment.orientation = startOrientation == UP ? DOWN : UP;
		this.endSegment.length = Math.max(0, sign * (endY - startY)) + this.connector.minSegmentLength;
	}
}


strategies[0] = function(connector) {return new HorizontalSStrategy(connector)};
strategies[1] = function(connector) {return new VerticalSStrategy(connector)};
strategies[2] = function(connector) {return new HorizontalLStrategy(connector)};
strategies[3] = function(connector) {return new VerticalLStrategy(connector)};
strategies[4] = function(connector) {return new HorizontalCStrategy(connector, LEFT)};
strategies[5] = function(connector) {return new HorizontalCStrategy(connector, RIGHT)};
strategies[6] = function(connector) {return new VerticalCStrategy(connector, UP)};
strategies[7] = function(connector) {return new VerticalCStrategy(connector, DOWN)};
