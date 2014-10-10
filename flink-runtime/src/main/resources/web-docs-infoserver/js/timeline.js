/**
 * @file timeline.js
 *
 * @brief
 * The Timeline is an interactive visualization chart to visualize events in
 * time, having a start and end date.
 * You can freely move and zoom in the timeline by dragging
 * and scrolling in the Timeline. Items are optionally dragable. The time
 * scale on the axis is adjusted automatically, and supports scales ranging
 * from milliseconds to years.
 *
 * Timeline is part of the CHAP Links library.
 *
 * Timeline is tested on Firefox 3.6, Safari 5.0, Chrome 6.0, Opera 10.6, and
 * Internet Explorer 6+.
 *
 * @license
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Copyright (c) 2011-2013 Almende B.V.
 *
 * @author     Jos de Jong, <jos@almende.org>
 * @date    2013-08-20
 * @version 2.5.0
 */

/*
 * i18n mods by github user iktuz (https://gist.github.com/iktuz/3749287/)
 * added to v2.4.1 with da_DK language by @bjarkebech
 */

/*
 * TODO
 *
 * Add zooming with pinching on Android
 * 
 * Bug: when an item contains a javascript onclick or a link, this does not work
 *      when the item is not selected (when the item is being selected,
 *      it is redrawn, which cancels any onclick or link action)
 * Bug: when an item contains an image without size, or a css max-width, it is not sized correctly
 * Bug: neglect items when they have no valid start/end, instead of throwing an error
 * Bug: Pinching on ipad does not work very well, sometimes the page will zoom when pinching vertically
 * Bug: cannot set max width for an item, like div.timeline-event-content {white-space: normal; max-width: 100px;}
 * Bug on IE in Quirks mode. When you have groups, and delete an item, the groups become invisible
 */

/**
 * Declare a unique namespace for CHAP's Common Hybrid Visualisation Library,
 * "links"
 */
if (typeof links === 'undefined') {
    links = {};
    // important: do not use var, as "var links = {};" will overwrite 
    //            the existing links variable value with undefined in IE8, IE7.  
}


/**
 * Ensure the variable google exists
 */
if (typeof google === 'undefined') {
    google = undefined;
    // important: do not use var, as "var google = undefined;" will overwrite 
    //            the existing google variable value with undefined in IE8, IE7.
}



// Internet Explorer 8 and older does not support Array.indexOf,
// so we define it here in that case
// http://soledadpenades.com/2007/05/17/arrayindexof-in-internet-explorer/
if(!Array.prototype.indexOf) {
    Array.prototype.indexOf = function(obj){
        for(var i = 0; i < this.length; i++){
            if(this[i] == obj){
                return i;
            }
        }
        return -1;
    }
}

// Internet Explorer 8 and older does not support Array.forEach,
// so we define it here in that case
// https://developer.mozilla.org/en-US/docs/JavaScript/Reference/Global_Objects/Array/forEach
if (!Array.prototype.forEach) {
    Array.prototype.forEach = function(fn, scope) {
        for(var i = 0, len = this.length; i < len; ++i) {
            fn.call(scope || this, this[i], i, this);
        }
    }
}


/**
 * @constructor links.Timeline
 * The timeline is a visualization chart to visualize events in time.
 *
 * The timeline is developed in javascript as a Google Visualization Chart.
 *
 * @param {Element} container   The DOM element in which the Timeline will
 *                                  be created. Normally a div element.
 */
links.Timeline = function(container) {
    if (!container) {
        // this call was probably only for inheritance, no constructor-code is required
        return;
    }

    // create variables and set default values
    this.dom = {};
    this.conversion = {};
    this.eventParams = {}; // stores parameters for mouse events
    this.groups = [];
    this.groupIndexes = {};
    this.items = [];
    this.renderQueue = {
        show: [],   // Items made visible but not yet added to DOM
        hide: [],   // Items currently visible but not yet removed from DOM
        update: []  // Items with changed data but not yet adjusted DOM
    };
    this.renderedItems = [];  // Items currently rendered in the DOM
    this.clusterGenerator = new links.Timeline.ClusterGenerator(this);
    this.currentClusters = [];
    this.selection = undefined; // stores index and item which is currently selected

    this.listeners = {}; // event listener callbacks

    // Initialize sizes. 
    // Needed for IE (which gives an error when you try to set an undefined
    // value in a style)
    this.size = {
        'actualHeight': 0,
        'axis': {
            'characterMajorHeight': 0,
            'characterMajorWidth': 0,
            'characterMinorHeight': 0,
            'characterMinorWidth': 0,
            'height': 0,
            'labelMajorTop': 0,
            'labelMinorTop': 0,
            'line': 0,
            'lineMajorWidth': 0,
            'lineMinorHeight': 0,
            'lineMinorTop': 0,
            'lineMinorWidth': 0,
            'top': 0
        },
        'contentHeight': 0,
        'contentLeft': 0,
        'contentWidth': 0,
        'frameHeight': 0,
        'frameWidth': 0,
        'groupsLeft': 0,
        'groupsWidth': 0,
        'items': {
            'top': 0
        }
    };

    this.dom.container = container;

    this.options = {
        'width': "100%",
        'height': "auto",
        'minHeight': 0,        // minimal height in pixels
        'autoHeight': true,

        'eventMargin': 10,     // minimal margin between events
        'eventMarginAxis': 20, // minimal margin between events and the axis
        'dragAreaWidth': 10,   // pixels

        'min': undefined,
        'max': undefined,
        'zoomMin': 10,     // milliseconds
        'zoomMax': 1000 * 60 * 60 * 24 * 365 * 10000, // milliseconds

        'moveable': true,
        'zoomable': true,
        'selectable': true,
        'unselectable': true,
        'editable': false,
        'snapEvents': true,
        'groupChangeable': true,

        'showCurrentTime': true, // show a red bar displaying the current time
        'showCustomTime': false, // show a blue, draggable bar displaying a custom time    
        'showMajorLabels': true,
        'showMinorLabels': true,
        'showNavigation': false,
        'showButtonNew': false,
        'groupsOnRight': false,
        'axisOnTop': false,
        'stackEvents': true,
        'animate': true,
        'animateZoom': true,
        'cluster': false,
        'style': 'box',
        'customStackOrder': false, //a function(a,b) for determining stackorder amongst a group of items. Essentially a comparator, -ve value for "a before b" and vice versa
        
        // i18n: Timeline only has built-in English text per default. Include timeline-locales.js to support more localized text.
        'locale': 'en',
        'MONTHS': new Array("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"),
        'MONTHS_SHORT': new Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        'DAYS': new Array("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"),
        'DAYS_SHORT': new Array("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"),
        'ZOOM_IN': "Zoom in",
        'ZOOM_OUT': "Zoom out",
        'MOVE_LEFT': "Move left",
        'MOVE_RIGHT': "Move right",
        'NEW': "New",
        'CREATE_NEW_EVENT': "Create new event"
    };

    this.clientTimeOffset = 0;    // difference between client time and the time
    // set via Timeline.setCurrentTime()
    var dom = this.dom;

    // remove all elements from the container element.
    while (dom.container.hasChildNodes()) {
        dom.container.removeChild(dom.container.firstChild);
    }

    // create a step for drawing the axis
    this.step = new links.Timeline.StepDate();

    // add standard item types
    this.itemTypes = {
        box:   links.Timeline.ItemBox,
        range: links.Timeline.ItemRange,
        dot:   links.Timeline.ItemDot
    };

    // initialize data
    this.data = [];
    this.firstDraw = true;

    // date interval must be initialized 
    this.setVisibleChartRange(undefined, undefined, false);

    // render for the first time
    this.render();

    // fire the ready event
    var me = this;
    setTimeout(function () {
        me.trigger('ready');
    }, 0);
};


/**
 * Main drawing logic. This is the function that needs to be called
 * in the html page, to draw the timeline.
 *
 * A data table with the events must be provided, and an options table.
 *
 * @param {google.visualization.DataTable}      data
 *                                 The data containing the events for the timeline.
 *                                 Object DataTable is defined in
 *                                 google.visualization.DataTable
 * @param {Object} options         A name/value map containing settings for the
 *                                 timeline. Optional.
 */
links.Timeline.prototype.draw = function(data, options) {
    this.setOptions(options);
    
    if (this.options.selectable) {
        links.Timeline.addClassName(this.dom.frame, "timeline-selectable");
    }

    // read the data
    this.setData(data);

    // set timer range. this will also redraw the timeline
    if (options && (options.start || options.end)) {
        this.setVisibleChartRange(options.start, options.end);
    }
    else if (this.firstDraw) {
        this.setVisibleChartRangeAuto();
    }

    this.firstDraw = false;
};


/**
 * Set options for the timeline.
 * Timeline must be redrawn afterwards
 * @param {Object} options A name/value map containing settings for the
 *                                 timeline. Optional.
 */
links.Timeline.prototype.setOptions = function(options) {
    if (options) {
        // retrieve parameter values
        for (var i in options) {
            if (options.hasOwnProperty(i)) {
                this.options[i] = options[i];
            }
        }
        
        // prepare i18n dependent on set locale
        if (typeof links.locales !== 'undefined' && this.options.locale !== 'en') {
            var localeOpts = links.locales[this.options.locale];
            if(localeOpts) {
                for (var l in localeOpts) {
                    if (localeOpts.hasOwnProperty(l)) {
                        this.options[l] = localeOpts[l];
                    }
                }
            }
        }

        // check for deprecated options
        if (options.showButtonAdd != undefined) {
            this.options.showButtonNew = options.showButtonAdd;
            console.log('WARNING: Option showButtonAdd is deprecated. Use showButtonNew instead');
        }
        if (options.intervalMin != undefined) {
            this.options.zoomMin = options.intervalMin;
            console.log('WARNING: Option intervalMin is deprecated. Use zoomMin instead');
        }
        if (options.intervalMax != undefined) {
            this.options.zoomMax = options.intervalMax;
            console.log('WARNING: Option intervalMax is deprecated. Use zoomMax instead');
        }

        if (options.scale && options.step) {
            this.step.setScale(options.scale, options.step);
        }
    }

    // validate options
    this.options.autoHeight = (this.options.height === "auto");
};

/**
 * Add new type of items
 * @param {String} typeName  Name of new type
 * @param {links.Timeline.Item} typeFactory Constructor of items
 */
links.Timeline.prototype.addItemType = function (typeName, typeFactory) {
    this.itemTypes[typeName] = typeFactory;
};

/**
 * Retrieve a map with the column indexes of the columns by column name.
 * For example, the method returns the map
 *     {
 *         start: 0,
 *         end: 1,
 *         content: 2,
 *         group: undefined,
 *         className: undefined
 *         editable: undefined
 *         type: undefined
 *     }
 * @param {google.visualization.DataTable} dataTable
 * @type {Object} map
 */
links.Timeline.mapColumnIds = function (dataTable) {
    var cols = {},
        colCount = dataTable.getNumberOfColumns(),
        allUndefined = true;

    // loop over the columns, and map the column id's to the column indexes
    for (var col = 0; col < colCount; col++) {
        var id = dataTable.getColumnId(col) || dataTable.getColumnLabel(col);
        cols[id] = col;
        if (id == 'start' || id == 'end' || id == 'content' || id == 'group' ||
            id == 'className' || id == 'editable' || id == 'type') {
            allUndefined = false;
        }
    }

    // if no labels or ids are defined, use the default mapping
    // for start, end, content, group, className, editable, type
    if (allUndefined) {
        cols.start = 0;
        cols.end = 1;
        cols.content = 2;
        if (colCount >= 3) {cols.group = 3}
        if (colCount >= 4) {cols.className = 4}
        if (colCount >= 5) {cols.editable = 5}
        if (colCount >= 6) {cols.type = 6}
    }

    return cols;
};

/**
 * Set data for the timeline
 * @param {google.visualization.DataTable | Array} data
 */
links.Timeline.prototype.setData = function(data) {
    // unselect any previously selected item
    this.unselectItem();

    if (!data) {
        data = [];
    }

    // clear all data
    this.stackCancelAnimation();
    this.clearItems();
    this.data = data;
    var items = this.items;
    this.deleteGroups();

    if (google && google.visualization &&
        data instanceof google.visualization.DataTable) {
        // map the datatable columns
        var cols = links.Timeline.mapColumnIds(data);

        // read DataTable
        for (var row = 0, rows = data.getNumberOfRows(); row < rows; row++) {
            items.push(this.createItem({
                'start':     ((cols.start != undefined)     ? data.getValue(row, cols.start)     : undefined),
                'end':       ((cols.end != undefined)       ? data.getValue(row, cols.end)       : undefined),
                'content':   ((cols.content != undefined)   ? data.getValue(row, cols.content)   : undefined),
                'group':     ((cols.group != undefined)     ? data.getValue(row, cols.group)     : undefined),
                'className': ((cols.className != undefined) ? data.getValue(row, cols.className) : undefined),
                'editable':  ((cols.editable != undefined)  ? data.getValue(row, cols.editable)  : undefined),
                'type':      ((cols.editable != undefined)  ? data.getValue(row, cols.type)      : undefined)
            }));
        }
    }
    else if (links.Timeline.isArray(data)) {
        // read JSON array
        for (var row = 0, rows = data.length; row < rows; row++) {
            var itemData = data[row];
            var item = this.createItem(itemData);
            items.push(item);
        }
    }
    else {
        throw "Unknown data type. DataTable or Array expected.";
    }

    // prepare data for clustering, by filtering and sorting by type
    if (this.options.cluster) {
        this.clusterGenerator.setData(this.items);
    }

    this.render({
        animate: false
    });
};

/**
 * Return the original data table.
 * @return {google.visualization.DataTable | Array} data
 */
links.Timeline.prototype.getData = function  () {
    return this.data;
};


/**
 * Update the original data with changed start, end or group.
 *
 * @param {Number} index
 * @param {Object} values   An object containing some of the following parameters:
 *                          {Date} start,
 *                          {Date} end,
 *                          {String} content,
 *                          {String} group
 */
links.Timeline.prototype.updateData = function  (index, values) {
    var data = this.data,
        prop;

    if (google && google.visualization &&
        data instanceof google.visualization.DataTable) {
        // update the original google DataTable
        var missingRows = (index + 1) - data.getNumberOfRows();
        if (missingRows > 0) {
            data.addRows(missingRows);
        }

        // map the column id's by name
        var cols = links.Timeline.mapColumnIds(data);

        // merge all fields from the provided data into the current data
        for (prop in values) {
            if (values.hasOwnProperty(prop)) {
                var col = cols[prop];
                if (col == undefined) {
                    // create new column
                    var value = values[prop];
                    var valueType = 'string';
                    if (typeof(value) == 'number')       {valueType = 'number';}
                    else if (typeof(value) == 'boolean') {valueType = 'boolean';}
                    else if (value instanceof Date)      {valueType = 'datetime';}
                    col = data.addColumn(valueType, prop);
                }
                data.setValue(index, col, values[prop]);

                // TODO: correctly serialize the start and end Date to the desired type (Date, String, or Number)
            }
        }
    }
    else if (links.Timeline.isArray(data)) {
        // update the original JSON table
        var row = data[index];
        if (row == undefined) {
            row = {};
            data[index] = row;
        }

        // merge all fields from the provided data into the current data
        for (prop in values) {
            if (values.hasOwnProperty(prop)) {
                row[prop] = values[prop];

                // TODO: correctly serialize the start and end Date to the desired type (Date, String, or Number)
            }
        }
    }
    else {
        throw "Cannot update data, unknown type of data";
    }
};

/**
 * Find the item index from a given HTML element
 * If no item index is found, undefined is returned
 * @param {Element} element
 * @return {Number | undefined} index
 */
links.Timeline.prototype.getItemIndex = function(element) {
    var e = element,
        dom = this.dom,
        frame = dom.items.frame,
        items = this.items,
        index = undefined;

    // try to find the frame where the items are located in
    while (e.parentNode && e.parentNode !== frame) {
        e = e.parentNode;
    }

    if (e.parentNode === frame) {
        // yes! we have found the parent element of all items
        // retrieve its id from the array with items
        for (var i = 0, iMax = items.length; i < iMax; i++) {
            if (items[i].dom === e) {
                index = i;
                break;
            }
        }
    }

    return index;
};

/**
 * Set a new size for the timeline
 * @param {string} width   Width in pixels or percentage (for example "800px"
 *                         or "50%")
 * @param {string} height  Height in pixels or percentage  (for example "400px"
 *                         or "30%")
 */
links.Timeline.prototype.setSize = function(width, height) {
    if (width) {
        this.options.width = width;
        this.dom.frame.style.width = width;
    }
    if (height) {
        this.options.height = height;
        this.options.autoHeight = (this.options.height === "auto");
        if (height !==  "auto" ) {
            this.dom.frame.style.height = height;
        }
    }

    this.render({
        animate: false
    });
};


/**
 * Set a new value for the visible range int the timeline.
 * Set start undefined to include everything from the earliest date to end.
 * Set end undefined to include everything from start to the last date.
 * Example usage:
 *    myTimeline.setVisibleChartRange(new Date("2010-08-22"),
 *                                    new Date("2010-09-13"));
 * @param {Date}   start     The start date for the timeline. optional
 * @param {Date}   end       The end date for the timeline. optional
 * @param {boolean} redraw   Optional. If true (default) the Timeline is
 *                           directly redrawn
 */
links.Timeline.prototype.setVisibleChartRange = function(start, end, redraw) {
    var range = {};
    if (!start || !end) {
        // retrieve the date range of the items
        range = this.getDataRange(true);
    }

    if (!start) {
        if (end) {
            if (range.min && range.min.valueOf() < end.valueOf()) {
                // start of the data
                start = range.min;
            }
            else {
                // 7 days before the end
                start = new Date(end.valueOf());
                start.setDate(start.getDate() - 7);
            }
        }
        else {
            // default of 3 days ago
            start = new Date();
            start.setDate(start.getDate() - 3);
        }
    }

    if (!end) {
        if (range.max) {
            // end of the data
            end = range.max;
        }
        else {
            // 7 days after start
            end = new Date(start.valueOf());
            end.setDate(end.getDate() + 7);
        }
    }

    // prevent start Date <= end Date
    if (end <= start) {
        end = new Date(start.valueOf());
        end.setDate(end.getDate() + 7);
    }

    // limit to the allowed range (don't let this do by applyRange,
    // because that method will try to maintain the interval (end-start)
    var min = this.options.min ? this.options.min : undefined; // date
    if (min != undefined && start.valueOf() < min.valueOf()) {
        start = new Date(min.valueOf()); // date
    }
    var max = this.options.max ? this.options.max : undefined; // date
    if (max != undefined && end.valueOf() > max.valueOf()) {
        end = new Date(max.valueOf()); // date
    }

    this.applyRange(start, end);

    if (redraw == undefined || redraw == true) {
        this.render({
            animate: false
        });  // TODO: optimize, no reflow needed
    }
    else {
        this.recalcConversion();
    }
};


/**
 * Change the visible chart range such that all items become visible
 */
links.Timeline.prototype.setVisibleChartRangeAuto = function() {
    var range = this.getDataRange(true);
    this.setVisibleChartRange(range.min, range.max);
};

/**
 * Adjust the visible range such that the current time is located in the center
 * of the timeline
 */
links.Timeline.prototype.setVisibleChartRangeNow = function() {
    var now = new Date();

    var diff = (this.end.valueOf() - this.start.valueOf());

    var startNew = new Date(now.valueOf() - diff/2);
    var endNew = new Date(startNew.valueOf() + diff);
    this.setVisibleChartRange(startNew, endNew);
};


/**
 * Retrieve the current visible range in the timeline.
 * @return {Object} An object with start and end properties
 */
links.Timeline.prototype.getVisibleChartRange = function() {
    return {
        'start': new Date(this.start.valueOf()),
        'end': new Date(this.end.valueOf())
    };
};

/**
 * Get the date range of the items.
 * @param {boolean} [withMargin]  If true, 5% of whitespace is added to the
 *                                left and right of the range. Default is false.
 * @return {Object} range    An object with parameters min and max.
 *                           - {Date} min is the lowest start date of the items
 *                           - {Date} max is the highest start or end date of the items
 *                           If no data is available, the values of min and max
 *                           will be undefined
 */
links.Timeline.prototype.getDataRange = function (withMargin) {
    var items = this.items,
        min = undefined, // number
        max = undefined; // number

    if (items) {
        for (var i = 0, iMax = items.length; i < iMax; i++) {
            var item = items[i],
                start = item.start != undefined ? item.start.valueOf() : undefined,
                end   = item.end != undefined   ? item.end.valueOf() : start;

            if (start != undefined) {
                min = (min != undefined) ? Math.min(min.valueOf(), start.valueOf()) : start;
            }

            if (end != undefined) {
                max = (max != undefined) ? Math.max(max.valueOf(), end.valueOf()) : end;
            }
        }
    }

    if (min && max && withMargin) {
        // zoom out 5% such that you have a little white space on the left and right
        var diff = (max - min);
        min = min - diff * 0.05;
        max = max + diff * 0.05;
    }

    return {
        'min': min != undefined ? new Date(min) : undefined,
        'max': max != undefined ? new Date(max) : undefined
    };
};

/**
 * Re-render (reflow and repaint) all components of the Timeline: frame, axis,
 * items, ...
 * @param {Object} [options]  Available options:
 *                            {boolean} renderTimesLeft   Number of times the
 *                                                        render may be repeated
 *                                                        5 times by default.
 *                            {boolean} animate           takes options.animate
 *                                                        as default value
 */
links.Timeline.prototype.render = function(options) {
    var frameResized = this.reflowFrame();
    var axisResized = this.reflowAxis();
    var groupsResized = this.reflowGroups();
    var itemsResized = this.reflowItems();
    var resized = (frameResized || axisResized || groupsResized || itemsResized);

    // TODO: only stackEvents/filterItems when resized or changed. (gives a bootstrap issue).
    // if (resized) {
    var animate = this.options.animate;
    if (options && options.animate != undefined) {
        animate = options.animate;
    }

    this.recalcConversion();
    this.clusterItems();
    this.filterItems();
    this.stackItems(animate);

    this.recalcItems();

    // TODO: only repaint when resized or when filterItems or stackItems gave a change?
    var needsReflow = this.repaint();

    // re-render once when needed (prevent endless re-render loop)
    if (needsReflow) {
        var renderTimesLeft = options ? options.renderTimesLeft : undefined;
        if (renderTimesLeft == undefined) {
            renderTimesLeft = 5;
        }
        if (renderTimesLeft > 0) {
            this.render({
                'animate': options ? options.animate: undefined,
                'renderTimesLeft': (renderTimesLeft - 1)
            });
        }
    }
};

/**
 * Repaint all components of the Timeline
 * @return {boolean} needsReflow   Returns true if the DOM is changed such that
 *                                 a reflow is needed.
 */
links.Timeline.prototype.repaint = function() {
    var frameNeedsReflow = this.repaintFrame();
    var axisNeedsReflow  = this.repaintAxis();
    var groupsNeedsReflow  = this.repaintGroups();
    var itemsNeedsReflow = this.repaintItems();
    this.repaintCurrentTime();
    this.repaintCustomTime();

    return (frameNeedsReflow || axisNeedsReflow || groupsNeedsReflow || itemsNeedsReflow);
};

/**
 * Reflow the timeline frame
 * @return {boolean} resized    Returns true if any of the frame elements
 *                              have been resized.
 */
links.Timeline.prototype.reflowFrame = function() {
    var dom = this.dom,
        options = this.options,
        size = this.size,
        resized = false;

    // Note: IE7 has issues with giving frame.clientWidth, therefore I use offsetWidth instead
    var frameWidth  = dom.frame ? dom.frame.offsetWidth : 0,
        frameHeight = dom.frame ? dom.frame.clientHeight : 0;

    resized = resized || (size.frameWidth !== frameWidth);
    resized = resized || (size.frameHeight !== frameHeight);
    size.frameWidth = frameWidth;
    size.frameHeight = frameHeight;

    return resized;
};

/**
 * repaint the Timeline frame
 * @return {boolean} needsReflow   Returns true if the DOM is changed such that
 *                                 a reflow is needed.
 */
links.Timeline.prototype.repaintFrame = function() {
    var needsReflow = false,
        dom = this.dom,
        options = this.options,
        size = this.size;

    // main frame
    if (!dom.frame) {
        dom.frame = document.createElement("DIV");
        dom.frame.className = "timeline-frame ui-widget ui-widget-content ui-corner-all";
        dom.frame.style.position = "relative";
        dom.frame.style.overflow = "hidden";
        dom.container.appendChild(dom.frame);
        needsReflow = true;
    }

    var height = options.autoHeight ?
        (size.actualHeight + "px") :
        (options.height || "100%");
    var width  = options.width || "100%";
    needsReflow = needsReflow || (dom.frame.style.height != height);
    needsReflow = needsReflow || (dom.frame.style.width != width);
    dom.frame.style.height = height;
    dom.frame.style.width = width;

    // contents
    if (!dom.content) {
        // create content box where the axis and items will be created
        dom.content = document.createElement("DIV");
        dom.content.style.position = "relative";
        dom.content.style.overflow = "hidden";
        dom.frame.appendChild(dom.content);

        var timelines = document.createElement("DIV");
        timelines.style.position = "absolute";
        timelines.style.left = "0px";
        timelines.style.top = "0px";
        timelines.style.height = "100%";
        timelines.style.width = "0px";
        dom.content.appendChild(timelines);
        dom.contentTimelines = timelines;

        var params = this.eventParams,
            me = this;
        if (!params.onMouseDown) {
            params.onMouseDown = function (event) {me.onMouseDown(event);};
            links.Timeline.addEventListener(dom.content, "mousedown", params.onMouseDown);
        }
        if (!params.onTouchStart) {
            params.onTouchStart = function (event) {me.onTouchStart(event);};
            links.Timeline.addEventListener(dom.content, "touchstart", params.onTouchStart);
        }
        if (!params.onMouseWheel) {
            params.onMouseWheel = function (event) {me.onMouseWheel(event);};
            links.Timeline.addEventListener(dom.content, "mousewheel", params.onMouseWheel);
        }
        if (!params.onDblClick) {
            params.onDblClick = function (event) {me.onDblClick(event);};
            links.Timeline.addEventListener(dom.content, "dblclick", params.onDblClick);
        }

        needsReflow = true;
    }
    dom.content.style.left = size.contentLeft + "px";
    dom.content.style.top = "0px";
    dom.content.style.width = size.contentWidth + "px";
    dom.content.style.height = size.frameHeight + "px";

    this.repaintNavigation();

    return needsReflow;
};

/**
 * Reflow the timeline axis. Calculate its height, width, positioning, etc...
 * @return {boolean} resized    returns true if the axis is resized
 */
links.Timeline.prototype.reflowAxis = function() {
    var resized = false,
        dom = this.dom,
        options = this.options,
        size = this.size,
        axisDom = dom.axis;

    var characterMinorWidth  = (axisDom && axisDom.characterMinor) ? axisDom.characterMinor.clientWidth : 0,
        characterMinorHeight = (axisDom && axisDom.characterMinor) ? axisDom.characterMinor.clientHeight : 0,
        characterMajorWidth  = (axisDom && axisDom.characterMajor) ? axisDom.characterMajor.clientWidth : 0,
        characterMajorHeight = (axisDom && axisDom.characterMajor) ? axisDom.characterMajor.clientHeight : 0,
        axisHeight = (options.showMinorLabels ? characterMinorHeight : 0) +
            (options.showMajorLabels ? characterMajorHeight : 0);

    var axisTop  = options.axisOnTop ? 0 : size.frameHeight - axisHeight,
        axisLine = options.axisOnTop ? axisHeight : axisTop;

    resized = resized || (size.axis.top !== axisTop);
    resized = resized || (size.axis.line !== axisLine);
    resized = resized || (size.axis.height !== axisHeight);
    size.axis.top = axisTop;
    size.axis.line = axisLine;
    size.axis.height = axisHeight;
    size.axis.labelMajorTop = options.axisOnTop ? 0 : axisLine +
        (options.showMinorLabels ? characterMinorHeight : 0);
    size.axis.labelMinorTop = options.axisOnTop ?
        (options.showMajorLabels ? characterMajorHeight : 0) :
        axisLine;
    size.axis.lineMinorTop = options.axisOnTop ? size.axis.labelMinorTop : 0;
    size.axis.lineMinorHeight = options.showMajorLabels ?
        size.frameHeight - characterMajorHeight:
        size.frameHeight;
    if (axisDom && axisDom.minorLines && axisDom.minorLines.length) {
        size.axis.lineMinorWidth = axisDom.minorLines[0].offsetWidth;
    }
    else {
        size.axis.lineMinorWidth = 1;
    }
    if (axisDom && axisDom.majorLines && axisDom.majorLines.length) {
        size.axis.lineMajorWidth = axisDom.majorLines[0].offsetWidth;
    }
    else {
        size.axis.lineMajorWidth = 1;
    }

    resized = resized || (size.axis.characterMinorWidth  !== characterMinorWidth);
    resized = resized || (size.axis.characterMinorHeight !== characterMinorHeight);
    resized = resized || (size.axis.characterMajorWidth  !== characterMajorWidth);
    resized = resized || (size.axis.characterMajorHeight !== characterMajorHeight);
    size.axis.characterMinorWidth  = characterMinorWidth;
    size.axis.characterMinorHeight = characterMinorHeight;
    size.axis.characterMajorWidth  = characterMajorWidth;
    size.axis.characterMajorHeight = characterMajorHeight;

    var contentHeight = Math.max(size.frameHeight - axisHeight, 0);
    size.contentLeft = options.groupsOnRight ? 0 : size.groupsWidth;
    size.contentWidth = Math.max(size.frameWidth - size.groupsWidth, 0);
    size.contentHeight = contentHeight;

    return resized;
};

/**
 * Redraw the timeline axis with minor and major labels
 * @return {boolean} needsReflow     Returns true if the DOM is changed such
 *                                   that a reflow is needed.
 */
links.Timeline.prototype.repaintAxis = function() {
    var needsReflow = false,
        dom = this.dom,
        options = this.options,
        size = this.size,
        step = this.step;

    var axis = dom.axis;
    if (!axis) {
        axis = {};
        dom.axis = axis;
    }
    if (!size.axis.properties) {
        size.axis.properties = {};
    }
    if (!axis.minorTexts) {
        axis.minorTexts = [];
    }
    if (!axis.minorLines) {
        axis.minorLines = [];
    }
    if (!axis.majorTexts) {
        axis.majorTexts = [];
    }
    if (!axis.majorLines) {
        axis.majorLines = [];
    }

    if (!axis.frame) {
        axis.frame = document.createElement("DIV");
        axis.frame.style.position = "absolute";
        axis.frame.style.left = "0px";
        axis.frame.style.top = "0px";
        dom.content.appendChild(axis.frame);
    }

    // take axis offline
    dom.content.removeChild(axis.frame);

    axis.frame.style.width = (size.contentWidth) + "px";
    axis.frame.style.height = (size.axis.height) + "px";

    // the drawn axis is more wide than the actual visual part, such that
    // the axis can be dragged without having to redraw it each time again.
    var start = this.screenToTime(0);
    var end = this.screenToTime(size.contentWidth);

    // calculate minimum step (in milliseconds) based on character size
    if (size.axis.characterMinorWidth) {
        this.minimumStep = this.screenToTime(size.axis.characterMinorWidth * 6) -
            this.screenToTime(0);

        step.setRange(start, end, this.minimumStep);
    }

    var charsNeedsReflow = this.repaintAxisCharacters();
    needsReflow = needsReflow || charsNeedsReflow;

    // The current labels on the axis will be re-used (much better performance),
    // therefore, the repaintAxis method uses the mechanism with
    // repaintAxisStartOverwriting, repaintAxisEndOverwriting, and
    // this.size.axis.properties is used.
    this.repaintAxisStartOverwriting();

    step.start();
    var xFirstMajorLabel = undefined;
    var max = 0;
    while (!step.end() && max < 1000) {
        max++;
        var cur = step.getCurrent(),
            x = this.timeToScreen(cur),
            isMajor = step.isMajor();

        if (options.showMinorLabels) {
            this.repaintAxisMinorText(x, step.getLabelMinor(options));
        }

        if (isMajor && options.showMajorLabels) {
            if (x > 0) {
                if (xFirstMajorLabel == undefined) {
                    xFirstMajorLabel = x;
                }
                this.repaintAxisMajorText(x, step.getLabelMajor(options));
            }
            this.repaintAxisMajorLine(x);
        }
        else {
            this.repaintAxisMinorLine(x);
        }

        step.next();
    }

    // create a major label on the left when needed
    if (options.showMajorLabels) {
        var leftTime = this.screenToTime(0),
            leftText = this.step.getLabelMajor(options, leftTime),
            width = leftText.length * size.axis.characterMajorWidth + 10; // upper bound estimation

        if (xFirstMajorLabel == undefined || width < xFirstMajorLabel) {
            this.repaintAxisMajorText(0, leftText, leftTime);
        }
    }

    // cleanup left over labels
    this.repaintAxisEndOverwriting();

    this.repaintAxisHorizontal();

    // put axis online
    dom.content.insertBefore(axis.frame, dom.content.firstChild);

    return needsReflow;
};

/**
 * Create characters used to determine the size of text on the axis
 * @return {boolean} needsReflow   Returns true if the DOM is changed such that
 *                                 a reflow is needed.
 */
links.Timeline.prototype.repaintAxisCharacters = function () {
    // calculate the width and height of a single character
    // this is used to calculate the step size, and also the positioning of the
    // axis
    var needsReflow = false,
        dom = this.dom,
        axis = dom.axis,
        text;

    if (!axis.characterMinor) {
        text = document.createTextNode("0");
        var characterMinor = document.createElement("DIV");
        characterMinor.className = "timeline-axis-text timeline-axis-text-minor";
        characterMinor.appendChild(text);
        characterMinor.style.position = "absolute";
        characterMinor.style.visibility = "hidden";
        characterMinor.style.paddingLeft = "0px";
        characterMinor.style.paddingRight = "0px";
        axis.frame.appendChild(characterMinor);

        axis.characterMinor = characterMinor;
        needsReflow = true;
    }

    if (!axis.characterMajor) {
        text = document.createTextNode("0");
        var characterMajor = document.createElement("DIV");
        characterMajor.className = "timeline-axis-text timeline-axis-text-major";
        characterMajor.appendChild(text);
        characterMajor.style.position = "absolute";
        characterMajor.style.visibility = "hidden";
        characterMajor.style.paddingLeft = "0px";
        characterMajor.style.paddingRight = "0px";
        axis.frame.appendChild(characterMajor);

        axis.characterMajor = characterMajor;
        needsReflow = true;
    }

    return needsReflow;
};

/**
 * Initialize redraw of the axis. All existing labels and lines will be
 * overwritten and reused.
 */
links.Timeline.prototype.repaintAxisStartOverwriting = function () {
    var properties = this.size.axis.properties;

    properties.minorTextNum = 0;
    properties.minorLineNum = 0;
    properties.majorTextNum = 0;
    properties.majorLineNum = 0;
};

/**
 * End of overwriting HTML DOM elements of the axis.
 * remaining elements will be removed
 */
links.Timeline.prototype.repaintAxisEndOverwriting = function () {
    var dom = this.dom,
        props = this.size.axis.properties,
        frame = this.dom.axis.frame,
        num;

    // remove leftovers
    var minorTexts = dom.axis.minorTexts;
    num = props.minorTextNum;
    while (minorTexts.length > num) {
        var minorText = minorTexts[num];
        frame.removeChild(minorText);
        minorTexts.splice(num, 1);
    }

    var minorLines = dom.axis.minorLines;
    num = props.minorLineNum;
    while (minorLines.length > num) {
        var minorLine = minorLines[num];
        frame.removeChild(minorLine);
        minorLines.splice(num, 1);
    }

    var majorTexts = dom.axis.majorTexts;
    num = props.majorTextNum;
    while (majorTexts.length > num) {
        var majorText = majorTexts[num];
        frame.removeChild(majorText);
        majorTexts.splice(num, 1);
    }

    var majorLines = dom.axis.majorLines;
    num = props.majorLineNum;
    while (majorLines.length > num) {
        var majorLine = majorLines[num];
        frame.removeChild(majorLine);
        majorLines.splice(num, 1);
    }
};

/**
 * Repaint the horizontal line and background of the axis
 */
links.Timeline.prototype.repaintAxisHorizontal = function() {
    var axis = this.dom.axis,
        size = this.size,
        options = this.options;

    // line behind all axis elements (possibly having a background color)
    var hasAxis = (options.showMinorLabels || options.showMajorLabels);
    if (hasAxis) {
        if (!axis.backgroundLine) {
            // create the axis line background (for a background color or so)
            var backgroundLine = document.createElement("DIV");
            backgroundLine.className = "timeline-axis";
            backgroundLine.style.position = "absolute";
            backgroundLine.style.left = "0px";
            backgroundLine.style.width = "100%";
            backgroundLine.style.border = "none";
            axis.frame.insertBefore(backgroundLine, axis.frame.firstChild);

            axis.backgroundLine = backgroundLine;
        }

        if (axis.backgroundLine) {
            axis.backgroundLine.style.top = size.axis.top + "px";
            axis.backgroundLine.style.height = size.axis.height + "px";
        }
    }
    else {
        if (axis.backgroundLine) {
            axis.frame.removeChild(axis.backgroundLine);
            delete axis.backgroundLine;
        }
    }

    // line before all axis elements
    if (hasAxis) {
        if (axis.line) {
            // put this line at the end of all childs
            var line = axis.frame.removeChild(axis.line);
            axis.frame.appendChild(line);
        }
        else {
            // make the axis line
            var line = document.createElement("DIV");
            line.className = "timeline-axis";
            line.style.position = "absolute";
            line.style.left = "0px";
            line.style.width = "100%";
            line.style.height = "0px";
            axis.frame.appendChild(line);

            axis.line = line;
        }

        axis.line.style.top = size.axis.line + "px";
    }
    else {
        if (axis.line && axis.line.parentElement) {
            axis.frame.removeChild(axis.line);
            delete axis.line;
        }
    }
};

/**
 * Create a minor label for the axis at position x
 * @param {Number} x
 * @param {String} text
 */
links.Timeline.prototype.repaintAxisMinorText = function (x, text) {
    var size = this.size,
        dom = this.dom,
        props = size.axis.properties,
        frame = dom.axis.frame,
        minorTexts = dom.axis.minorTexts,
        index = props.minorTextNum,
        label;

    if (index < minorTexts.length) {
        label = minorTexts[index]
    }
    else {
        // create new label
        var content = document.createTextNode("");
        label = document.createElement("DIV");
        label.appendChild(content);
        label.className = "timeline-axis-text timeline-axis-text-minor";
        label.style.position = "absolute";

        frame.appendChild(label);

        minorTexts.push(label);
    }

    label.childNodes[0].nodeValue = text;
    label.style.left = x + "px";
    label.style.top  = size.axis.labelMinorTop + "px";
    //label.title = title;  // TODO: this is a heavy operation

    props.minorTextNum++;
};

/**
 * Create a minor line for the axis at position x
 * @param {Number} x
 */
links.Timeline.prototype.repaintAxisMinorLine = function (x) {
    var axis = this.size.axis,
        dom = this.dom,
        props = axis.properties,
        frame = dom.axis.frame,
        minorLines = dom.axis.minorLines,
        index = props.minorLineNum,
        line;

    if (index < minorLines.length) {
        line = minorLines[index];
    }
    else {
        // create vertical line
        line = document.createElement("DIV");
        line.className = "timeline-axis-grid timeline-axis-grid-minor";
        line.style.position = "absolute";
        line.style.width = "0px";

        frame.appendChild(line);
        minorLines.push(line);
    }

    line.style.top = axis.lineMinorTop + "px";
    line.style.height = axis.lineMinorHeight + "px";
    line.style.left = (x - axis.lineMinorWidth/2) + "px";

    props.minorLineNum++;
};

/**
 * Create a Major label for the axis at position x
 * @param {Number} x
 * @param {String} text
 */
links.Timeline.prototype.repaintAxisMajorText = function (x, text) {
    var size = this.size,
        props = size.axis.properties,
        frame = this.dom.axis.frame,
        majorTexts = this.dom.axis.majorTexts,
        index = props.majorTextNum,
        label;

    if (index < majorTexts.length) {
        label = majorTexts[index];
    }
    else {
        // create label
        var content = document.createTextNode(text);
        label = document.createElement("DIV");
        label.className = "timeline-axis-text timeline-axis-text-major";
        label.appendChild(content);
        label.style.position = "absolute";
        label.style.top = "0px";

        frame.appendChild(label);
        majorTexts.push(label);
    }

    label.childNodes[0].nodeValue = text;
    label.style.top = size.axis.labelMajorTop + "px";
    label.style.left = x + "px";
    //label.title = title; // TODO: this is a heavy operation

    props.majorTextNum ++;
};

/**
 * Create a Major line for the axis at position x
 * @param {Number} x
 */
links.Timeline.prototype.repaintAxisMajorLine = function (x) {
    var size = this.size,
        props = size.axis.properties,
        axis = this.size.axis,
        frame = this.dom.axis.frame,
        majorLines = this.dom.axis.majorLines,
        index = props.majorLineNum,
        line;

    if (index < majorLines.length) {
        line = majorLines[index];
    }
    else {
        // create vertical line
        line = document.createElement("DIV");
        line.className = "timeline-axis-grid timeline-axis-grid-major";
        line.style.position = "absolute";
        line.style.top = "0px";
        line.style.width = "0px";

        frame.appendChild(line);
        majorLines.push(line);
    }

    line.style.left = (x - axis.lineMajorWidth/2) + "px";
    line.style.height = size.frameHeight + "px";

    props.majorLineNum ++;
};

/**
 * Reflow all items, retrieve their actual size
 * @return {boolean} resized    returns true if any of the items is resized
 */
links.Timeline.prototype.reflowItems = function() {
    var resized = false,
        i,
        iMax,
        group,
        groups = this.groups,
        renderedItems = this.renderedItems;

    if (groups) { // TODO: need to check if labels exists?
        // loop through all groups to reset the items height
        groups.forEach(function (group) {
            group.itemsHeight = 0;
        });
    }

    // loop through the width and height of all visible items
    for (i = 0, iMax = renderedItems.length; i < iMax; i++) {
        var item = renderedItems[i],
            domItem = item.dom;
        group = item.group;

        if (domItem) {
            // TODO: move updating width and height into item.reflow
            var width = domItem ? domItem.clientWidth : 0;
            var height = domItem ? domItem.clientHeight : 0;
            resized = resized || (item.width != width);
            resized = resized || (item.height != height);
            item.width = width;
            item.height = height;
            //item.borderWidth = (domItem.offsetWidth - domItem.clientWidth - 2) / 2; // TODO: borderWidth
            item.reflow();
        }

        if (group) {
            group.itemsHeight = group.itemsHeight ?
                Math.max(group.itemsHeight, item.height) :
                item.height;
        }
    }

    return resized;
};

/**
 * Recalculate item properties:
 * - the height of each group.
 * - the actualHeight, from the stacked items or the sum of the group heights
 * @return {boolean} resized    returns true if any of the items properties is
 *                              changed
 */
links.Timeline.prototype.recalcItems = function () {
    var resized = false,
        i,
        iMax,
        item,
        finalItem,
        finalItems,
        group,
        groups = this.groups,
        size = this.size,
        options = this.options,
        renderedItems = this.renderedItems;

    var actualHeight = 0;
    if (groups.length == 0) {
        // calculate actual height of the timeline when there are no groups
        // but stacked items
        if (options.autoHeight || options.cluster) {
            var min = 0,
                max = 0;

            if (this.stack && this.stack.finalItems) {
                // adjust the offset of all finalItems when the actualHeight has been changed
                finalItems = this.stack.finalItems;
                finalItem = finalItems[0];
                if (finalItem && finalItem.top) {
                    min = finalItem.top;
                    max = finalItem.top + finalItem.height;
                }
                for (i = 1, iMax = finalItems.length; i < iMax; i++) {
                    finalItem = finalItems[i];
                    min = Math.min(min, finalItem.top);
                    max = Math.max(max, finalItem.top + finalItem.height);
                }
            }
            else {
                item = renderedItems[0];
                if (item && item.top) {
                    min = item.top;
                    max = item.top + item.height;
                }
                for (i = 1, iMax = renderedItems.length; i < iMax; i++) {
                    item = renderedItems[i];
                    if (item.top) {
                        min = Math.min(min, item.top);
                        max = Math.max(max, (item.top + item.height));
                    }
                }
            }

            actualHeight = (max - min) + 2 * options.eventMarginAxis + size.axis.height;
            if (actualHeight < options.minHeight) {
                actualHeight = options.minHeight;
            }

            if (size.actualHeight != actualHeight && options.autoHeight && !options.axisOnTop) {
                // adjust the offset of all items when the actualHeight has been changed
                var diff = actualHeight - size.actualHeight;
                if (this.stack && this.stack.finalItems) {
                    finalItems = this.stack.finalItems;
                    for (i = 0, iMax = finalItems.length; i < iMax; i++) {
                        finalItems[i].top += diff;
                        finalItems[i].item.top += diff;
                    }
                }
                else {
                    for (i = 0, iMax = renderedItems.length; i < iMax; i++) {
                        renderedItems[i].top += diff;
                    }
                }
            }
        }
    }
    else {
        // loop through all groups to get the height of each group, and the
        // total height
        actualHeight = size.axis.height + 2 * options.eventMarginAxis;
        for (i = 0, iMax = groups.length; i < iMax; i++) {
            group = groups[i];

            var groupHeight = Math.max(group.labelHeight || 0, group.itemsHeight || 0);
            resized = resized || (groupHeight != group.height);
            group.height = groupHeight;

            actualHeight += groups[i].height + options.eventMargin;
        }

        // calculate top positions of the group labels and lines
        var eventMargin = options.eventMargin,
            top = options.axisOnTop ?
                options.eventMarginAxis + eventMargin/2 :
                size.contentHeight - options.eventMarginAxis + eventMargin/ 2,
            axisHeight = size.axis.height;

        for (i = 0, iMax = groups.length; i < iMax; i++) {
            group = groups[i];
            if (options.axisOnTop) {
                group.top = top + axisHeight;
                group.labelTop = top + axisHeight + (group.height - group.labelHeight) / 2;
                group.lineTop = top + axisHeight + group.height + eventMargin/2;
                top += group.height + eventMargin;
            }
            else {
                top -= group.height + eventMargin;
                group.top = top;
                group.labelTop = top + (group.height - group.labelHeight) / 2;
                group.lineTop = top - eventMargin/2;
            }
        }

        // calculate top position of the visible items
        for (i = 0, iMax = renderedItems.length; i < iMax; i++) {
            item = renderedItems[i];
            group = item.group;

            if (group) {
                item.top = group.top;
            }
        }

        resized = true;
    }

    if (actualHeight < options.minHeight) {
        actualHeight = options.minHeight;
    }
    resized = resized || (actualHeight != size.actualHeight);
    size.actualHeight = actualHeight;

    return resized;
};

/**
 * This method clears the (internal) array this.items in a safe way: neatly
 * cleaning up the DOM, and accompanying arrays this.renderedItems and
 * the created clusters.
 */
links.Timeline.prototype.clearItems = function() {
    // add all visible items to the list to be hidden
    var hideItems = this.renderQueue.hide;
    this.renderedItems.forEach(function (item) {
        hideItems.push(item);
    });

    // clear the cluster generator
    this.clusterGenerator.clear();

    // actually clear the items
    this.items = [];
};

/**
 * Repaint all items
 * @return {boolean} needsReflow   Returns true if the DOM is changed such that
 *                                 a reflow is needed.
 */
links.Timeline.prototype.repaintItems = function() {
    var i, iMax, item, index;

    var needsReflow = false,
        dom = this.dom,
        size = this.size,
        timeline = this,
        renderedItems = this.renderedItems;

    if (!dom.items) {
        dom.items = {};
    }

    // draw the frame containing the items
    var frame = dom.items.frame;
    if (!frame) {
        frame = document.createElement("DIV");
        frame.style.position = "relative";
        dom.content.appendChild(frame);
        dom.items.frame = frame;
    }

    frame.style.left = "0px";
    frame.style.top = size.items.top + "px";
    frame.style.height = "0px";

    // Take frame offline (for faster manipulation of the DOM)
    dom.content.removeChild(frame);

    // process the render queue with changes
    var queue = this.renderQueue;
    var newImageUrls = [];
    needsReflow = needsReflow ||
        (queue.show.length > 0) ||
        (queue.update.length > 0) ||
        (queue.hide.length > 0);   // TODO: reflow needed on hide of items?

    while (item = queue.show.shift()) {
        item.showDOM(frame);
        item.getImageUrls(newImageUrls);
        renderedItems.push(item);
    }
    while (item = queue.update.shift()) {
        item.updateDOM(frame);
        item.getImageUrls(newImageUrls);
        index = this.renderedItems.indexOf(item);
        if (index == -1) {
            renderedItems.push(item);
        }
    }
    while (item = queue.hide.shift()) {
        item.hideDOM(frame);
        index = this.renderedItems.indexOf(item);
        if (index != -1) {
            renderedItems.splice(index, 1);
        }
    }

    // reposition all visible items
    renderedItems.forEach(function (item) {
        item.updatePosition(timeline);
    });

    // redraw the delete button and dragareas of the selected item (if any)
    this.repaintDeleteButton();
    this.repaintDragAreas();

    // put frame online again
    dom.content.appendChild(frame);

    if (newImageUrls.length) {
        // retrieve all image sources from the items, and set a callback once
        // all images are retrieved
        var callback = function () {
            timeline.render();
        };
        var sendCallbackWhenAlreadyLoaded = false;
        links.imageloader.loadAll(newImageUrls, callback, sendCallbackWhenAlreadyLoaded);
    }

    return needsReflow;
};

/**
 * Reflow the size of the groups
 * @return {boolean} resized    Returns true if any of the frame elements
 *                              have been resized.
 */
links.Timeline.prototype.reflowGroups = function() {
    var resized = false,
        options = this.options,
        size = this.size,
        dom = this.dom;

    // calculate the groups width and height
    // TODO: only update when data is changed! -> use an updateSeq
    var groupsWidth = 0;

    // loop through all groups to get the labels width and height
    var groups = this.groups;
    var labels = this.dom.groups ? this.dom.groups.labels : [];
    for (var i = 0, iMax = groups.length; i < iMax; i++) {
        var group = groups[i];
        var label = labels[i];
        group.labelWidth  = label ? label.clientWidth : 0;
        group.labelHeight = label ? label.clientHeight : 0;
        group.width = group.labelWidth;  // TODO: group.width is redundant with labelWidth

        groupsWidth = Math.max(groupsWidth, group.width);
    }

    // limit groupsWidth to the groups width in the options
    if (options.groupsWidth !== undefined) {
        groupsWidth = dom.groups.frame ? dom.groups.frame.clientWidth : 0;
    }

    // compensate for the border width. TODO: calculate the real border width
    groupsWidth += 1;

    var groupsLeft = options.groupsOnRight ? size.frameWidth - groupsWidth : 0;
    resized = resized || (size.groupsWidth !== groupsWidth);
    resized = resized || (size.groupsLeft !== groupsLeft);
    size.groupsWidth = groupsWidth;
    size.groupsLeft = groupsLeft;

    return resized;
};

/**
 * Redraw the group labels
 */
links.Timeline.prototype.repaintGroups = function() {
    var dom = this.dom,
        timeline = this,
        options = this.options,
        size = this.size,
        groups = this.groups;

    if (dom.groups === undefined) {
        dom.groups = {};
    }

    var labels = dom.groups.labels;
    if (!labels) {
        labels = [];
        dom.groups.labels = labels;
    }
    var labelLines = dom.groups.labelLines;
    if (!labelLines) {
        labelLines = [];
        dom.groups.labelLines = labelLines;
    }
    var itemLines = dom.groups.itemLines;
    if (!itemLines) {
        itemLines = [];
        dom.groups.itemLines = itemLines;
    }

    // create the frame for holding the groups
    var frame = dom.groups.frame;
    if (!frame) {
        frame =  document.createElement("DIV");
        frame.className = "timeline-groups-axis";
        frame.style.position = "absolute";
        frame.style.overflow = "hidden";
        frame.style.top = "0px";
        frame.style.height = "100%";

        dom.frame.appendChild(frame);
        dom.groups.frame = frame;
    }

    frame.style.left = size.groupsLeft + "px";
    frame.style.width = (options.groupsWidth !== undefined) ?
        options.groupsWidth :
        size.groupsWidth + "px";

    // hide groups axis when there are no groups
    if (groups.length == 0) {
        frame.style.display = 'none';
    }
    else {
        frame.style.display = '';
    }

    // TODO: only create/update groups when data is changed.

    // create the items
    var current = labels.length,
        needed = groups.length;

    // overwrite existing group labels
    for (var i = 0, iMax = Math.min(current, needed); i < iMax; i++) {
        var group = groups[i];
        var label = labels[i];
        label.innerHTML = this.getGroupName(group);
        label.style.display = '';
    }

    // append new items when needed
    for (var i = current; i < needed; i++) {
        var group = groups[i];

        // create text label
        var label = document.createElement("DIV");
        label.className = "timeline-groups-text";
        label.style.position = "absolute";
        if (options.groupsWidth === undefined) {
            label.style.whiteSpace = "nowrap";
        }
        label.innerHTML = this.getGroupName(group);
        frame.appendChild(label);
        labels[i] = label;

        // create the grid line between the group labels
        var labelLine = document.createElement("DIV");
        labelLine.className = "timeline-axis-grid timeline-axis-grid-minor";
        labelLine.style.position = "absolute";
        labelLine.style.left = "0px";
        labelLine.style.width = "100%";
        labelLine.style.height = "0px";
        labelLine.style.borderTopStyle = "solid";
        frame.appendChild(labelLine);
        labelLines[i] = labelLine;

        // create the grid line between the items
        var itemLine = document.createElement("DIV");
        itemLine.className = "timeline-axis-grid timeline-axis-grid-minor";
        itemLine.style.position = "absolute";
        itemLine.style.left = "0px";
        itemLine.style.width = "100%";
        itemLine.style.height = "0px";
        itemLine.style.borderTopStyle = "solid";
        dom.content.insertBefore(itemLine, dom.content.firstChild);
        itemLines[i] = itemLine;
    }

    // remove redundant items from the DOM when needed
    for (var i = needed; i < current; i++) {
        var label = labels[i],
            labelLine = labelLines[i],
            itemLine = itemLines[i];

        frame.removeChild(label);
        frame.removeChild(labelLine);
        dom.content.removeChild(itemLine);
    }
    labels.splice(needed, current - needed);
    labelLines.splice(needed, current - needed);
    itemLines.splice(needed, current - needed);
    
    links.Timeline.addClassName(frame, options.groupsOnRight ? 'timeline-groups-axis-onright' : 'timeline-groups-axis-onleft');

    // position the groups
    for (var i = 0, iMax = groups.length; i < iMax; i++) {
        var group = groups[i],
            label = labels[i],
            labelLine = labelLines[i],
            itemLine = itemLines[i];

        label.style.top = group.labelTop + "px";
        labelLine.style.top = group.lineTop + "px";
        itemLine.style.top = group.lineTop + "px";
        itemLine.style.width = size.contentWidth + "px";
    }

    if (!dom.groups.background) {
        // create the axis grid line background
        var background = document.createElement("DIV");
        background.className = "timeline-axis";
        background.style.position = "absolute";
        background.style.left = "0px";
        background.style.width = "100%";
        background.style.border = "none";

        frame.appendChild(background);
        dom.groups.background = background;
    }
    dom.groups.background.style.top = size.axis.top + 'px';
    dom.groups.background.style.height = size.axis.height + 'px';

    if (!dom.groups.line) {
        // create the axis grid line
        var line = document.createElement("DIV");
        line.className = "timeline-axis";
        line.style.position = "absolute";
        line.style.left = "0px";
        line.style.width = "100%";
        line.style.height = "0px";

        frame.appendChild(line);
        dom.groups.line = line;
    }
    dom.groups.line.style.top = size.axis.line + 'px';

    // create a callback when there are images which are not yet loaded
    // TODO: more efficiently load images in the groups
    if (dom.groups.frame && groups.length) {
        var imageUrls = [];
        links.imageloader.filterImageUrls(dom.groups.frame, imageUrls);
        if (imageUrls.length) {
            // retrieve all image sources from the items, and set a callback once
            // all images are retrieved
            var callback = function () {
                timeline.render();
            };
            var sendCallbackWhenAlreadyLoaded = false;
            links.imageloader.loadAll(imageUrls, callback, sendCallbackWhenAlreadyLoaded);
        }
    }
};


/**
 * Redraw the current time bar
 */
links.Timeline.prototype.repaintCurrentTime = function() {
    var options = this.options,
        dom = this.dom,
        size = this.size;

    if (!options.showCurrentTime) {
        if (dom.currentTime) {
            dom.contentTimelines.removeChild(dom.currentTime);
            delete dom.currentTime;
        }

        return;
    }

    if (!dom.currentTime) {
        // create the current time bar
        var currentTime = document.createElement("DIV");
        currentTime.className = "timeline-currenttime";
        currentTime.style.position = "absolute";
        currentTime.style.top = "0px";
        currentTime.style.height = "100%";

        dom.contentTimelines.appendChild(currentTime);
        dom.currentTime = currentTime;
    }

    var now = new Date();
    var nowOffset = new Date(now.valueOf() + this.clientTimeOffset);
    var x = this.timeToScreen(nowOffset);

    var visible = (x > -size.contentWidth && x < 2 * size.contentWidth);
    dom.currentTime.style.display = visible ? '' : 'none';
    dom.currentTime.style.left = x + "px";
    dom.currentTime.title = "Current time: " + nowOffset;

    // start a timer to adjust for the new time
    if (this.currentTimeTimer != undefined) {
        clearTimeout(this.currentTimeTimer);
        delete this.currentTimeTimer;
    }
    var timeline = this;
    var onTimeout = function() {
        timeline.repaintCurrentTime();
    };
    // the time equal to the width of one pixel, divided by 2 for more smoothness
    var interval = 1 / this.conversion.factor / 2;
    if (interval < 30) interval = 30;
    this.currentTimeTimer = setTimeout(onTimeout, interval);
};

/**
 * Redraw the custom time bar
 */
links.Timeline.prototype.repaintCustomTime = function() {
    var options = this.options,
        dom = this.dom,
        size = this.size;

    if (!options.showCustomTime) {
        if (dom.customTime) {
            dom.contentTimelines.removeChild(dom.customTime);
            delete dom.customTime;
        }

        return;
    }

    if (!dom.customTime) {
        var customTime = document.createElement("DIV");
        customTime.className = "timeline-customtime";
        customTime.style.position = "absolute";
        customTime.style.top = "0px";
        customTime.style.height = "100%";

        var drag = document.createElement("DIV");
        drag.style.position = "relative";
        drag.style.top = "0px";
        drag.style.left = "-10px";
        drag.style.height = "100%";
        drag.style.width = "20px";
        customTime.appendChild(drag);

        dom.contentTimelines.appendChild(customTime);
        dom.customTime = customTime;

        // initialize parameter
        this.customTime = new Date();
    }

    var x = this.timeToScreen(this.customTime),
        visible = (x > -size.contentWidth && x < 2 * size.contentWidth);
    dom.customTime.style.display = visible ? '' : 'none';
    dom.customTime.style.left = x + "px";
    dom.customTime.title = "Time: " + this.customTime;
};


/**
 * Redraw the delete button, on the top right of the currently selected item
 * if there is no item selected, the button is hidden.
 */
links.Timeline.prototype.repaintDeleteButton = function () {
    var timeline = this,
        dom = this.dom,
        frame = dom.items.frame;

    var deleteButton = dom.items.deleteButton;
    if (!deleteButton) {
        // create a delete button
        deleteButton = document.createElement("DIV");
        deleteButton.className = "timeline-navigation-delete";
        deleteButton.style.position = "absolute";

        frame.appendChild(deleteButton);
        dom.items.deleteButton = deleteButton;
    }

    var index = this.selection ? this.selection.index : -1,
        item = this.selection ? this.items[index] : undefined;
    if (item && item.rendered && this.isEditable(item)) {
        var right = item.getRight(this),
            top = item.top;

        deleteButton.style.left = right + 'px';
        deleteButton.style.top = top + 'px';
        deleteButton.style.display = '';
        frame.removeChild(deleteButton);
        frame.appendChild(deleteButton);
    }
    else {
        deleteButton.style.display = 'none';
    }
};


/**
 * Redraw the drag areas. When an item (ranges only) is selected,
 * it gets a drag area on the left and right side, to change its width
 */
links.Timeline.prototype.repaintDragAreas = function () {
    var timeline = this,
        options = this.options,
        dom = this.dom,
        frame = this.dom.items.frame;

    // create left drag area
    var dragLeft = dom.items.dragLeft;
    if (!dragLeft) {
        dragLeft = document.createElement("DIV");
        dragLeft.className="timeline-event-range-drag-left";
        dragLeft.style.position = "absolute";

        frame.appendChild(dragLeft);
        dom.items.dragLeft = dragLeft;
    }

    // create right drag area
    var dragRight = dom.items.dragRight;
    if (!dragRight) {
        dragRight = document.createElement("DIV");
        dragRight.className="timeline-event-range-drag-right";
        dragRight.style.position = "absolute";

        frame.appendChild(dragRight);
        dom.items.dragRight = dragRight;
    }

    // reposition left and right drag area
    var index = this.selection ? this.selection.index : -1,
        item = this.selection ? this.items[index] : undefined;
    if (item && item.rendered && this.isEditable(item) &&
        (item instanceof links.Timeline.ItemRange)) {
        var left = this.timeToScreen(item.start),
            right = this.timeToScreen(item.end),
            top = item.top,
            height = item.height;

        dragLeft.style.left = left + 'px';
        dragLeft.style.top = top + 'px';
        dragLeft.style.width = options.dragAreaWidth + "px";
        dragLeft.style.height = height + 'px';
        dragLeft.style.display = '';
        frame.removeChild(dragLeft);
        frame.appendChild(dragLeft);

        dragRight.style.left = (right - options.dragAreaWidth) + 'px';
        dragRight.style.top = top + 'px';
        dragRight.style.width = options.dragAreaWidth + "px";
        dragRight.style.height = height + 'px';
        dragRight.style.display = '';
        frame.removeChild(dragRight);
        frame.appendChild(dragRight);
    }
    else {
        dragLeft.style.display = 'none';
        dragRight.style.display = 'none';
    }
};

/**
 * Create the navigation buttons for zooming and moving
 */
links.Timeline.prototype.repaintNavigation = function () {
    var timeline = this,
        options = this.options,
        dom = this.dom,
        frame = dom.frame,
        navBar = dom.navBar;

    if (!navBar) {
        var showButtonNew = options.showButtonNew && options.editable;
        var showNavigation = options.showNavigation && (options.zoomable || options.moveable);
        if (showNavigation || showButtonNew) {
            // create a navigation bar containing the navigation buttons
            navBar = document.createElement("DIV");
            navBar.style.position = "absolute";
            navBar.className = "timeline-navigation ui-widget ui-state-highlight ui-corner-all";
            if (options.groupsOnRight) {
                navBar.style.left = '10px';
            }
            else {
                navBar.style.right = '10px';
            }
            if (options.axisOnTop) {
                navBar.style.bottom = '10px';
            }
            else {
                navBar.style.top = '10px';
            }
            dom.navBar = navBar;
            frame.appendChild(navBar);
        }

        if (showButtonNew) {
            // create a new in button
            navBar.addButton = document.createElement("DIV");
            navBar.addButton.className = "timeline-navigation-new";
            navBar.addButton.title = options.CREATE_NEW_EVENT;
            var addIconSpan = document.createElement("SPAN");
            addIconSpan.className = "ui-icon ui-icon-circle-plus";            
            navBar.addButton.appendChild(addIconSpan);
            
            var onAdd = function(event) {
                links.Timeline.preventDefault(event);
                links.Timeline.stopPropagation(event);

                // create a new event at the center of the frame
                var w = timeline.size.contentWidth;
                var x = w / 2;
                var xstart = timeline.screenToTime(x - w / 10); // subtract 10% of timeline width
                var xend = timeline.screenToTime(x + w / 10);   // add 10% of timeline width
                if (options.snapEvents) {
                    timeline.step.snap(xstart);
                    timeline.step.snap(xend);
                }

                var content = options.NEW;
                var group = timeline.groups.length ? timeline.groups[0].content : undefined;
                var preventRender = true;
                timeline.addItem({
                    'start': xstart,
                    'end': xend,
                    'content': content,
                    'group': group
                }, preventRender);
                var index = (timeline.items.length - 1);
                timeline.selectItem(index);

                timeline.applyAdd = true;

                // fire an add event.
                // Note that the change can be canceled from within an event listener if
                // this listener calls the method cancelAdd().
                timeline.trigger('add');

                if (timeline.applyAdd) {
                    // render and select the item
                    timeline.render({animate: false});
                    timeline.selectItem(index);
                }
                else {
                    // undo an add
                    timeline.deleteItem(index);
                }
            };
            links.Timeline.addEventListener(navBar.addButton, "mousedown", onAdd);
            navBar.appendChild(navBar.addButton);
        }

        if (showButtonNew && showNavigation) {
            // create a separator line
            links.Timeline.addClassName(navBar.addButton, 'timeline-navigation-new-line');
        }

        if (showNavigation) {
            if (options.zoomable) {
                // create a zoom in button
                navBar.zoomInButton = document.createElement("DIV");
                navBar.zoomInButton.className = "timeline-navigation-zoom-in";
                navBar.zoomInButton.title = this.options.ZOOM_IN;
                var ziIconSpan = document.createElement("SPAN");
                ziIconSpan.className = "ui-icon ui-icon-circle-zoomin";
                navBar.zoomInButton.appendChild(ziIconSpan);
                
                var onZoomIn = function(event) {
                    links.Timeline.preventDefault(event);
                    links.Timeline.stopPropagation(event);
                    timeline.zoom(0.4);
                    timeline.trigger("rangechange");
                    timeline.trigger("rangechanged");
                };
                links.Timeline.addEventListener(navBar.zoomInButton, "mousedown", onZoomIn);
                navBar.appendChild(navBar.zoomInButton);

                // create a zoom out button
                navBar.zoomOutButton = document.createElement("DIV");
                navBar.zoomOutButton.className = "timeline-navigation-zoom-out";
                navBar.zoomOutButton.title = this.options.ZOOM_OUT;
                var zoIconSpan = document.createElement("SPAN");
                zoIconSpan.className = "ui-icon ui-icon-circle-zoomout";
                navBar.zoomOutButton.appendChild(zoIconSpan);
                
                var onZoomOut = function(event) {
                    links.Timeline.preventDefault(event);
                    links.Timeline.stopPropagation(event);
                    timeline.zoom(-0.4);
                    timeline.trigger("rangechange");
                    timeline.trigger("rangechanged");
                };
                links.Timeline.addEventListener(navBar.zoomOutButton, "mousedown", onZoomOut);
                navBar.appendChild(navBar.zoomOutButton);
            }

            if (options.moveable) {
                // create a move left button
                navBar.moveLeftButton = document.createElement("DIV");
                navBar.moveLeftButton.className = "timeline-navigation-move-left";
                navBar.moveLeftButton.title = this.options.MOVE_LEFT;
                var mlIconSpan = document.createElement("SPAN");
                mlIconSpan.className = "ui-icon ui-icon-circle-arrow-w";
                navBar.moveLeftButton.appendChild(mlIconSpan);
                
                var onMoveLeft = function(event) {
                    links.Timeline.preventDefault(event);
                    links.Timeline.stopPropagation(event);
                    timeline.move(-0.2);
                    timeline.trigger("rangechange");
                    timeline.trigger("rangechanged");
                };
                links.Timeline.addEventListener(navBar.moveLeftButton, "mousedown", onMoveLeft);
                navBar.appendChild(navBar.moveLeftButton);

                // create a move right button
                navBar.moveRightButton = document.createElement("DIV");
                navBar.moveRightButton.className = "timeline-navigation-move-right";
                navBar.moveRightButton.title = this.options.MOVE_RIGHT;
                var mrIconSpan = document.createElement("SPAN");
                mrIconSpan.className = "ui-icon ui-icon-circle-arrow-e";
                navBar.moveRightButton.appendChild(mrIconSpan);
                
                var onMoveRight = function(event) {
                    links.Timeline.preventDefault(event);
                    links.Timeline.stopPropagation(event);
                    timeline.move(0.2);
                    timeline.trigger("rangechange");
                    timeline.trigger("rangechanged");
                };
                links.Timeline.addEventListener(navBar.moveRightButton, "mousedown", onMoveRight);
                navBar.appendChild(navBar.moveRightButton);
            }
        }
    }
};


/**
 * Set current time. This function can be used to set the time in the client
 * timeline equal with the time on a server.
 * @param {Date} time
 */
links.Timeline.prototype.setCurrentTime = function(time) {
    var now = new Date();
    this.clientTimeOffset = (time.valueOf() - now.valueOf());

    this.repaintCurrentTime();
};

/**
 * Get current time. The time can have an offset from the real time, when
 * the current time has been changed via the method setCurrentTime.
 * @return {Date} time
 */
links.Timeline.prototype.getCurrentTime = function() {
    var now = new Date();
    return new Date(now.valueOf() + this.clientTimeOffset);
};


/**
 * Set custom time.
 * The custom time bar can be used to display events in past or future.
 * @param {Date} time
 */
links.Timeline.prototype.setCustomTime = function(time) {
    this.customTime = new Date(time.valueOf());
    this.repaintCustomTime();
};

/**
 * Retrieve the current custom time.
 * @return {Date} customTime
 */
links.Timeline.prototype.getCustomTime = function() {
    return new Date(this.customTime.valueOf());
};

/**
 * Set a custom scale. Autoscaling will be disabled.
 * For example setScale(SCALE.MINUTES, 5) will result
 * in minor steps of 5 minutes, and major steps of an hour.
 *
 * @param {links.Timeline.StepDate.SCALE} scale
 *                               A scale. Choose from SCALE.MILLISECOND,
 *                               SCALE.SECOND, SCALE.MINUTE, SCALE.HOUR,
 *                               SCALE.WEEKDAY, SCALE.DAY, SCALE.MONTH,
 *                               SCALE.YEAR.
 * @param {int}        step   A step size, by default 1. Choose for
 *                               example 1, 2, 5, or 10.
 */
links.Timeline.prototype.setScale = function(scale, step) {
    this.step.setScale(scale, step);
    this.render(); // TODO: optimize: only reflow/repaint axis
};

/**
 * Enable or disable autoscaling
 * @param {boolean} enable  If true or not defined, autoscaling is enabled.
 *                          If false, autoscaling is disabled.
 */
links.Timeline.prototype.setAutoScale = function(enable) {
    this.step.setAutoScale(enable);
    this.render(); // TODO: optimize: only reflow/repaint axis
};

/**
 * Redraw the timeline
 * Reloads the (linked) data table and redraws the timeline when resized.
 * See also the method checkResize
 */
links.Timeline.prototype.redraw = function() {
    this.setData(this.data);
};


/**
 * Check if the timeline is resized, and if so, redraw the timeline.
 * Useful when the webpage is resized.
 */
links.Timeline.prototype.checkResize = function() {
    // TODO: re-implement the method checkResize, or better, make it redundant as this.render will be smarter
    this.render();
};

/**
 * Check whether a given item is editable
 * @param {links.Timeline.Item} item
 * @return {boolean} editable
 */
links.Timeline.prototype.isEditable = function (item) {
    if (item) {
        if (item.editable != undefined) {
            return item.editable;
        }
        else {
            return this.options.editable;
        }
    }
    return false;
};

/**
 * Calculate the factor and offset to convert a position on screen to the
 * corresponding date and vice versa.
 * After the method calcConversionFactor is executed once, the methods screenToTime and
 * timeToScreen can be used.
 */
links.Timeline.prototype.recalcConversion = function() {
    this.conversion.offset = this.start.valueOf();
    this.conversion.factor = this.size.contentWidth /
        (this.end.valueOf() - this.start.valueOf());
};


/**
 * Convert a position on screen (pixels) to a datetime
 * Before this method can be used, the method calcConversionFactor must be
 * executed once.
 * @param {int}     x    Position on the screen in pixels
 * @return {Date}   time The datetime the corresponds with given position x
 */
links.Timeline.prototype.screenToTime = function(x) {
    var conversion = this.conversion;
    return new Date(x / conversion.factor + conversion.offset);
};

/**
 * Convert a datetime (Date object) into a position on the screen
 * Before this method can be used, the method calcConversionFactor must be
 * executed once.
 * @param {Date}   time A date
 * @return {int}   x    The position on the screen in pixels which corresponds
 *                      with the given date.
 */
links.Timeline.prototype.timeToScreen = function(time) {
    var conversion = this.conversion;
    return (time.valueOf() - conversion.offset) * conversion.factor;
};



/**
 * Event handler for touchstart event on mobile devices
 */
links.Timeline.prototype.onTouchStart = function(event) {
    var params = this.eventParams,
        me = this;

    if (params.touchDown) {
        // if already moving, return
        return;
    }

    params.touchDown = true;
    params.zoomed = false;

    this.onMouseDown(event);

    if (!params.onTouchMove) {
        params.onTouchMove = function (event) {me.onTouchMove(event);};
        links.Timeline.addEventListener(document, "touchmove", params.onTouchMove);
    }
    if (!params.onTouchEnd) {
        params.onTouchEnd  = function (event) {me.onTouchEnd(event);};
        links.Timeline.addEventListener(document, "touchend",  params.onTouchEnd);
    }

    /* TODO
     // check for double tap event
     var delta = 500; // ms
     var doubleTapStart = (new Date()).valueOf();
     var target = links.Timeline.getTarget(event);
     var doubleTapItem = this.getItemIndex(target);
     if (params.doubleTapStart &&
     (doubleTapStart - params.doubleTapStart) < delta &&
     doubleTapItem == params.doubleTapItem) {
     delete params.doubleTapStart;
     delete params.doubleTapItem;
     me.onDblClick(event);
     params.touchDown = false;
     }
     params.doubleTapStart = doubleTapStart;
     params.doubleTapItem = doubleTapItem;
     */
    // store timing for double taps
    var target = links.Timeline.getTarget(event);
    var item = this.getItemIndex(target);
    params.doubleTapStartPrev = params.doubleTapStart;
    params.doubleTapStart = (new Date()).valueOf();
    params.doubleTapItemPrev = params.doubleTapItem;
    params.doubleTapItem = item;

    links.Timeline.preventDefault(event);
};

/**
 * Event handler for touchmove event on mobile devices
 */
links.Timeline.prototype.onTouchMove = function(event) {
    var params = this.eventParams;

    if (event.scale && event.scale !== 1) {
        params.zoomed = true;
    }

    if (!params.zoomed) {
        // move 
        this.onMouseMove(event);
    }
    else {
        if (this.options.zoomable) {
            // pinch
            // TODO: pinch only supported on iPhone/iPad. Create something manually for Android?
            params.zoomed = true;

            var scale = event.scale,
                oldWidth = (params.end.valueOf() - params.start.valueOf()),
                newWidth = oldWidth / scale,
                diff = newWidth - oldWidth,
                start = new Date(parseInt(params.start.valueOf() - diff/2)),
                end = new Date(parseInt(params.end.valueOf() + diff/2));

            // TODO: determine zoom-around-date from touch positions?

            this.setVisibleChartRange(start, end);
            this.trigger("rangechange");
        }
    }

    links.Timeline.preventDefault(event);
};

/**
 * Event handler for touchend event on mobile devices
 */
links.Timeline.prototype.onTouchEnd = function(event) {
    var params = this.eventParams;
    var me = this;
    params.touchDown = false;

    if (params.zoomed) {
        this.trigger("rangechanged");
    }

    if (params.onTouchMove) {
        links.Timeline.removeEventListener(document, "touchmove", params.onTouchMove);
        delete params.onTouchMove;

    }
    if (params.onTouchEnd) {
        links.Timeline.removeEventListener(document, "touchend",  params.onTouchEnd);
        delete params.onTouchEnd;
    }

    this.onMouseUp(event);

    // check for double tap event
    var delta = 500; // ms
    var doubleTapEnd = (new Date()).valueOf();
    var target = links.Timeline.getTarget(event);
    var doubleTapItem = this.getItemIndex(target);
    if (params.doubleTapStartPrev &&
        (doubleTapEnd - params.doubleTapStartPrev) < delta &&
        params.doubleTapItem == params.doubleTapItemPrev) {
        params.touchDown = true;
        me.onDblClick(event);
        params.touchDown = false;
    }

    links.Timeline.preventDefault(event);
};


/**
 * Start a moving operation inside the provided parent element
 * @param {Event} event       The event that occurred (required for
 *                             retrieving the  mouse position)
 */
links.Timeline.prototype.onMouseDown = function(event) {
    event = event || window.event;

    var params = this.eventParams,
        options = this.options,
        dom = this.dom;

    // only react on left mouse button down
    var leftButtonDown = event.which ? (event.which == 1) : (event.button == 1);
    if (!leftButtonDown && !params.touchDown) {
        return;
    }

    // get mouse position
    params.mouseX = links.Timeline.getPageX(event);
    params.mouseY = links.Timeline.getPageY(event);
    params.frameLeft = links.Timeline.getAbsoluteLeft(this.dom.content);
    params.frameTop = links.Timeline.getAbsoluteTop(this.dom.content);
    params.previousLeft = 0;
    params.previousOffset = 0;

    params.moved = false;
    params.start = new Date(this.start.valueOf());
    params.end = new Date(this.end.valueOf());

    params.target = links.Timeline.getTarget(event);
    var dragLeft = (dom.items && dom.items.dragLeft) ? dom.items.dragLeft : undefined;
    var dragRight = (dom.items && dom.items.dragRight) ? dom.items.dragRight : undefined;
    params.itemDragLeft = (params.target === dragLeft);
    params.itemDragRight = (params.target === dragRight);

    if (params.itemDragLeft || params.itemDragRight) {
        params.itemIndex = this.selection ? this.selection.index : undefined;
    }
    else {
        params.itemIndex = this.getItemIndex(params.target);
    }

    params.customTime = (params.target === dom.customTime ||
        params.target.parentNode === dom.customTime) ?
        this.customTime :
        undefined;

    params.addItem = (options.editable && event.ctrlKey);
    if (params.addItem) {
        // create a new event at the current mouse position
        var x = params.mouseX - params.frameLeft;
        var y = params.mouseY - params.frameTop;

        var xstart = this.screenToTime(x);
        if (options.snapEvents) {
            this.step.snap(xstart);
        }
        var xend = new Date(xstart.valueOf());
        var content = options.NEW;
        var group = this.getGroupFromHeight(y);
        this.addItem({
            'start': xstart,
            'end': xend,
            'content': content,
            'group': this.getGroupName(group)
        });
        params.itemIndex = (this.items.length - 1);
        this.selectItem(params.itemIndex);
        params.itemDragRight = true;
    }

    var item = this.items[params.itemIndex];
    var isSelected = this.isSelected(params.itemIndex);
    params.editItem = isSelected && this.isEditable(item);
    if (params.editItem) {
        params.itemStart = item.start;
        params.itemEnd = item.end;
        params.itemGroup = item.group;
        params.itemLeft = item.start ? this.timeToScreen(item.start) : undefined;
        params.itemRight = item.end ? this.timeToScreen(item.end) : undefined;
    }
    else {
        this.dom.frame.style.cursor = 'move';
    }
    if (!params.touchDown) {
        // add event listeners to handle moving the contents
        // we store the function onmousemove and onmouseup in the timeline, so we can
        // remove the eventlisteners lateron in the function mouseUp()
        var me = this;
        if (!params.onMouseMove) {
            params.onMouseMove = function (event) {me.onMouseMove(event);};
            links.Timeline.addEventListener(document, "mousemove", params.onMouseMove);
        }
        if (!params.onMouseUp) {
            params.onMouseUp = function (event) {me.onMouseUp(event);};
            links.Timeline.addEventListener(document, "mouseup", params.onMouseUp);
        }

        links.Timeline.preventDefault(event);
    }
};


/**
 * Perform moving operating.
 * This function activated from within the funcion links.Timeline.onMouseDown().
 * @param {Event}   event  Well, eehh, the event
 */
links.Timeline.prototype.onMouseMove = function (event) {
    event = event || window.event;

    var params = this.eventParams,
        size = this.size,
        dom = this.dom,
        options = this.options;

    // calculate change in mouse position
    var mouseX = links.Timeline.getPageX(event);
    var mouseY = links.Timeline.getPageY(event);

    if (params.mouseX == undefined) {
        params.mouseX = mouseX;
    }
    if (params.mouseY == undefined) {
        params.mouseY = mouseY;
    }

    var diffX = mouseX - params.mouseX;
    var diffY = mouseY - params.mouseY;

    // if mouse movement is big enough, register it as a "moved" event
    if (Math.abs(diffX) >= 1) {
        params.moved = true;
    }

    if (params.customTime) {
        var x = this.timeToScreen(params.customTime);
        var xnew = x + diffX;
        this.customTime = this.screenToTime(xnew);
        this.repaintCustomTime();

        // fire a timechange event
        this.trigger('timechange');
    }
    else if (params.editItem) {
        var item = this.items[params.itemIndex],
            left,
            right;

        if (params.itemDragLeft) {
            // move the start of the item
            left = params.itemLeft + diffX;
            right = params.itemRight;

            item.start = this.screenToTime(left);
            if (options.snapEvents) {
                this.step.snap(item.start);
                left = this.timeToScreen(item.start);
            }

            if (left > right) {
                left = right;
                item.start = this.screenToTime(left);
            }
        }
        else if (params.itemDragRight) {
            // move the end of the item
            left = params.itemLeft;
            right = params.itemRight + diffX;

            item.end = this.screenToTime(right);
            if (options.snapEvents) {
                this.step.snap(item.end);
                right = this.timeToScreen(item.end);
            }

            if (right < left) {
                right = left;
                item.end = this.screenToTime(right);
            }
        }
        else {
            // move the item
            left = params.itemLeft + diffX;
            item.start = this.screenToTime(left);
            if (options.snapEvents) {
                this.step.snap(item.start);
                left = this.timeToScreen(item.start);
            }

            if (item.end) {
                right = left + (params.itemRight - params.itemLeft);
                item.end = this.screenToTime(right);
            }
        }

        item.setPosition(left, right);

        var dragging = params.itemDragLeft || params.itemDragRight;
        if (this.groups.length && !dragging) {
            // move item from one group to another when needed
            var y = mouseY - params.frameTop;
            var group = this.getGroupFromHeight(y);
            if (options.groupsChangeable && item.group !== group) {
                // move item to the other group
                var index = this.items.indexOf(item);
                this.changeItem(index, {'group': this.getGroupName(group)});
            }
            else {
                this.repaintDeleteButton();
                this.repaintDragAreas();
            }
        }
        else {
            // TODO: does not work well in FF, forces redraw with every mouse move it seems
            this.render(); // TODO: optimize, only redraw the items?
            // Note: when animate==true, no redraw is needed here, its done by stackItems animation
        }
    }
    else if (options.moveable) {
        var interval = (params.end.valueOf() - params.start.valueOf());
        var diffMillisecs = Math.round((-diffX) / size.contentWidth * interval);
        var newStart = new Date(params.start.valueOf() + diffMillisecs);
        var newEnd = new Date(params.end.valueOf() + diffMillisecs);
        this.applyRange(newStart, newEnd);
        // if the applied range is moved due to a fixed min or max,
        // change the diffMillisecs accordingly
        var appliedDiff = (this.start.valueOf() - newStart.valueOf());
        if (appliedDiff) {
            diffMillisecs += appliedDiff;
        }

        this.recalcConversion();

        // move the items by changing the left position of their frame.
        // this is much faster than repositioning all elements individually via the 
        // repaintFrame() function (which is done once at mouseup)
        // note that we round diffX to prevent wrong positioning on millisecond scale
        var previousLeft = params.previousLeft || 0;
        var currentLeft = parseFloat(dom.items.frame.style.left) || 0;
        var previousOffset = params.previousOffset || 0;
        var frameOffset = previousOffset + (currentLeft - previousLeft);
        var frameLeft = -diffMillisecs / interval * size.contentWidth + frameOffset;

        dom.items.frame.style.left = (frameLeft) + "px";

        // read the left again from DOM (IE8- rounds the value)
        params.previousOffset = frameOffset;
        params.previousLeft = parseFloat(dom.items.frame.style.left) || frameLeft;

        this.repaintCurrentTime();
        this.repaintCustomTime();
        this.repaintAxis();

        // fire a rangechange event
        this.trigger('rangechange');
    }

    links.Timeline.preventDefault(event);
};


/**
 * Stop moving operating.
 * This function activated from within the funcion links.Timeline.onMouseDown().
 * @param {event}  event   The event
 */
links.Timeline.prototype.onMouseUp = function (event) {
    var params = this.eventParams,
        options = this.options;

    event = event || window.event;

    this.dom.frame.style.cursor = 'auto';

    // remove event listeners here, important for Safari
    if (params.onMouseMove) {
        links.Timeline.removeEventListener(document, "mousemove", params.onMouseMove);
        delete params.onMouseMove;
    }
    if (params.onMouseUp) {
        links.Timeline.removeEventListener(document, "mouseup",   params.onMouseUp);
        delete params.onMouseUp;
    }
    //links.Timeline.preventDefault(event);

    if (params.customTime) {
        // fire a timechanged event
        this.trigger('timechanged');
    }
    else if (params.editItem) {
        var item = this.items[params.itemIndex];

        if (params.moved || params.addItem) {
            this.applyChange = true;
            this.applyAdd = true;

            this.updateData(params.itemIndex, {
                'start': item.start,
                'end': item.end
            });

            // fire an add or change event. 
            // Note that the change can be canceled from within an event listener if 
            // this listener calls the method cancelChange().
            this.trigger(params.addItem ? 'add' : 'change');

            if (params.addItem) {
                if (this.applyAdd) {
                    this.updateData(params.itemIndex, {
                        'start': item.start,
                        'end': item.end,
                        'content': item.content,
                        'group': this.getGroupName(item.group)
                    });
                }
                else {
                    // undo an add
                    this.deleteItem(params.itemIndex);
                }
            }
            else {
                if (this.applyChange) {
                    this.updateData(params.itemIndex, {
                        'start': item.start,
                        'end': item.end
                    });
                }
                else {
                    // undo a change
                    delete this.applyChange;
                    delete this.applyAdd;

                    var item = this.items[params.itemIndex],
                        domItem = item.dom;

                    item.start = params.itemStart;
                    item.end = params.itemEnd;
                    item.group = params.itemGroup;
                    // TODO: original group should be restored too
                    item.setPosition(params.itemLeft, params.itemRight);
                }
            }

            // prepare data for clustering, by filtering and sorting by type
            if (this.options.cluster) {
                this.clusterGenerator.updateData();
            }

            this.render();
        }
    }
    else {
        if (!params.moved && !params.zoomed) {
            // mouse did not move -> user has selected an item

            if (params.target === this.dom.items.deleteButton) {
                // delete item
                if (this.selection) {
                    this.confirmDeleteItem(this.selection.index);
                }
            }
            else if (options.selectable) {
                // select/unselect item
                if (params.itemIndex != undefined) {
                    if (!this.isSelected(params.itemIndex)) {
                        this.selectItem(params.itemIndex);
                        this.trigger('select');
                    }
                }
                else {
                    if (options.unselectable) {
                        this.unselectItem();
                        this.trigger('select');
                    }
                }
            }
        }
        else {
            // timeline is moved
            // TODO: optimize: no need to reflow and cluster again?
            this.render();

            if ((params.moved && options.moveable) || (params.zoomed && options.zoomable) ) {
                // fire a rangechanged event
                this.trigger('rangechanged');
            }
        }
    }
};

/**
 * Double click event occurred for an item
 * @param {Event}  event
 */
links.Timeline.prototype.onDblClick = function (event) {
    var params = this.eventParams,
        options = this.options,
        dom = this.dom,
        size = this.size;
    event = event || window.event;

    if (params.itemIndex != undefined) {
        var item = this.items[params.itemIndex];
        if (item && this.isEditable(item)) {
            // fire the edit event
            this.trigger('edit');
        }
    }
    else {
        if (options.editable) {
            // create a new item

            // get mouse position
            params.mouseX = links.Timeline.getPageX(event);
            params.mouseY = links.Timeline.getPageY(event);
            var x = params.mouseX - links.Timeline.getAbsoluteLeft(dom.content);
            var y = params.mouseY - links.Timeline.getAbsoluteTop(dom.content);

            // create a new event at the current mouse position
            var xstart = this.screenToTime(x);
            var xend = this.screenToTime(x  + size.frameWidth / 10); // add 10% of timeline width
            if (options.snapEvents) {
                this.step.snap(xstart);
                this.step.snap(xend);
            }

            var content = options.NEW;
            var group = this.getGroupFromHeight(y);   // (group may be undefined)
            var preventRender = true;
            this.addItem({
                'start': xstart,
                'end': xend,
                'content': content,
                'group': this.getGroupName(group)
            }, preventRender);
            params.itemIndex = (this.items.length - 1);
            this.selectItem(params.itemIndex);

            this.applyAdd = true;

            // fire an add event.
            // Note that the change can be canceled from within an event listener if
            // this listener calls the method cancelAdd().
            this.trigger('add');

            if (this.applyAdd) {
                // render and select the item
                this.render({animate: false});
                this.selectItem(params.itemIndex);
            }
            else {
                // undo an add
                this.deleteItem(params.itemIndex);
            }
        }
    }

    links.Timeline.preventDefault(event);
};


/**
 * Event handler for mouse wheel event, used to zoom the timeline
 * Code from http://adomas.org/javascript-mouse-wheel/
 * @param {Event}  event   The event
 */
links.Timeline.prototype.onMouseWheel = function(event) {
    if (!this.options.zoomable)
        return;

    if (!event) { /* For IE. */
        event = window.event;
    }

    // retrieve delta    
    var delta = 0;
    if (event.wheelDelta) { /* IE/Opera. */
        delta = event.wheelDelta/120;
    } else if (event.detail) { /* Mozilla case. */
        // In Mozilla, sign of delta is different than in IE.
        // Also, delta is multiple of 3.
        delta = -event.detail/3;
    }

    // If delta is nonzero, handle it.
    // Basically, delta is now positive if wheel was scrolled up,
    // and negative, if wheel was scrolled down.
    if (delta) {
        // TODO: on FireFox, the window is not redrawn within repeated scroll-events 
        // -> use a delayed redraw? Make a zoom queue?

        var timeline = this;
        var zoom = function () {
            // perform the zoom action. Delta is normally 1 or -1
            var zoomFactor = delta / 5.0;
            var frameLeft = links.Timeline.getAbsoluteLeft(timeline.dom.content);
            var mouseX = links.Timeline.getPageX(event);
            var zoomAroundDate =
                (mouseX != undefined && frameLeft != undefined) ?
                    timeline.screenToTime(mouseX - frameLeft) :
                    undefined;

            timeline.zoom(zoomFactor, zoomAroundDate);

            // fire a rangechange and a rangechanged event
            timeline.trigger("rangechange");
            timeline.trigger("rangechanged");
        };

        var scroll = function () {
            // Scroll the timeline
            timeline.move(delta * -0.2);
            timeline.trigger("rangechange");
            timeline.trigger("rangechanged");
        };

        if (event.shiftKey) {
            scroll();
        }
        else {
            zoom();
        }
    }

    // Prevent default actions caused by mouse wheel.
    // That might be ugly, but we handle scrolls somehow
    // anyway, so don't bother here...
    links.Timeline.preventDefault(event);
};


/**
 * Zoom the timeline the given zoomfactor in or out. Start and end date will
 * be adjusted, and the timeline will be redrawn. You can optionally give a
 * date around which to zoom.
 * For example, try zoomfactor = 0.1 or -0.1
 * @param {Number} zoomFactor      Zooming amount. Positive value will zoom in,
 *                                 negative value will zoom out
 * @param {Date}   zoomAroundDate  Date around which will be zoomed. Optional
 */
links.Timeline.prototype.zoom = function(zoomFactor, zoomAroundDate) {
    // if zoomAroundDate is not provided, take it half between start Date and end Date
    if (zoomAroundDate == undefined) {
        zoomAroundDate = new Date((this.start.valueOf() + this.end.valueOf()) / 2);
    }

    // prevent zoom factor larger than 1 or smaller than -1 (larger than 1 will
    // result in a start>=end )
    if (zoomFactor >= 1) {
        zoomFactor = 0.9;
    }
    if (zoomFactor <= -1) {
        zoomFactor = -0.9;
    }

    // adjust a negative factor such that zooming in with 0.1 equals zooming
    // out with a factor -0.1
    if (zoomFactor < 0) {
        zoomFactor = zoomFactor / (1 + zoomFactor);
    }

    // zoom start Date and end Date relative to the zoomAroundDate
    var startDiff = (this.start.valueOf() - zoomAroundDate);
    var endDiff = (this.end.valueOf() - zoomAroundDate);

    // calculate new dates
    var newStart = new Date(this.start.valueOf() - startDiff * zoomFactor);
    var newEnd   = new Date(this.end.valueOf() - endDiff * zoomFactor);

    // only zoom in when interval is larger than minimum interval (to prevent
    // sliding to left/right when having reached the minimum zoom level)
    var interval = (newEnd.valueOf() - newStart.valueOf());
    var zoomMin = Number(this.options.zoomMin) || 10;
    if (zoomMin < 10) {
        zoomMin = 10;
    }
    if (interval >= zoomMin) {
        this.applyRange(newStart, newEnd, zoomAroundDate);
        this.render({
            animate: this.options.animate && this.options.animateZoom
        });
    }
};

/**
 * Move the timeline the given movefactor to the left or right. Start and end
 * date will be adjusted, and the timeline will be redrawn.
 * For example, try moveFactor = 0.1 or -0.1
 * @param {Number}  moveFactor      Moving amount. Positive value will move right,
 *                                 negative value will move left
 */
links.Timeline.prototype.move = function(moveFactor) {
    // zoom start Date and end Date relative to the zoomAroundDate
    var diff = (this.end.valueOf() - this.start.valueOf());

    // apply new dates
    var newStart = new Date(this.start.valueOf() + diff * moveFactor);
    var newEnd   = new Date(this.end.valueOf() + diff * moveFactor);
    this.applyRange(newStart, newEnd);

    this.render(); // TODO: optimize, no need to reflow, only to recalc conversion and repaint
};

/**
 * Apply a visible range. The range is limited to feasible maximum and minimum
 * range.
 * @param {Date} start
 * @param {Date} end
 * @param {Date}   zoomAroundDate  Optional. Date around which will be zoomed.
 */
links.Timeline.prototype.applyRange = function (start, end, zoomAroundDate) {
    // calculate new start and end value
    var startValue = start.valueOf(); // number
    var endValue = end.valueOf();     // number
    var interval = (endValue - startValue);

    // determine maximum and minimum interval
    var options = this.options;
    var year = 1000 * 60 * 60 * 24 * 365;
    var zoomMin = Number(options.zoomMin) || 10;
    if (zoomMin < 10) {
        zoomMin = 10;
    }
    var zoomMax = Number(options.zoomMax) || 10000 * year;
    if (zoomMax > 10000 * year) {
        zoomMax = 10000 * year;
    }
    if (zoomMax < zoomMin) {
        zoomMax = zoomMin;
    }

    // determine min and max date value
    var min = options.min ? options.min.valueOf() : undefined; // number
    var max = options.max ? options.max.valueOf() : undefined; // number
    if (min != undefined && max != undefined) {
        if (min >= max) {
            // empty range
            var day = 1000 * 60 * 60 * 24;
            max = min + day;
        }
        if (zoomMax > (max - min)) {
            zoomMax = (max - min);
        }
        if (zoomMin > (max - min)) {
            zoomMin = (max - min);
        }
    }

    // prevent empty interval
    if (startValue >= endValue) {
        endValue += 1000 * 60 * 60 * 24;
    }

    // prevent too small scale
    // TODO: IE has problems with milliseconds
    if (interval < zoomMin) {
        var diff = (zoomMin - interval);
        var f = zoomAroundDate ? (zoomAroundDate.valueOf() - startValue) / interval : 0.5;
        startValue -= Math.round(diff * f);
        endValue   += Math.round(diff * (1 - f));
    }

    // prevent too large scale
    if (interval > zoomMax) {
        var diff = (interval - zoomMax);
        var f = zoomAroundDate ? (zoomAroundDate.valueOf() - startValue) / interval : 0.5;
        startValue += Math.round(diff * f);
        endValue   -= Math.round(diff * (1 - f));
    }

    // prevent to small start date
    if (min != undefined) {
        var diff = (startValue - min);
        if (diff < 0) {
            startValue -= diff;
            endValue -= diff;
        }
    }

    // prevent to large end date
    if (max != undefined) {
        var diff = (max - endValue);
        if (diff < 0) {
            startValue += diff;
            endValue += diff;
        }
    }

    // apply new dates
    this.start = new Date(startValue);
    this.end = new Date(endValue);
};

/**
 * Delete an item after a confirmation.
 * The deletion can be cancelled by executing .cancelDelete() during the
 * triggered event 'delete'.
 * @param {int} index   Index of the item to be deleted
 */
links.Timeline.prototype.confirmDeleteItem = function(index) {
    this.applyDelete = true;

    // select the event to be deleted
    if (!this.isSelected(index)) {
        this.selectItem(index);
    }

    // fire a delete event trigger. 
    // Note that the delete event can be canceled from within an event listener if 
    // this listener calls the method cancelChange().
    this.trigger('delete');

    if (this.applyDelete) {
        this.deleteItem(index);
    }

    delete this.applyDelete;
};

/**
 * Delete an item
 * @param {int} index   Index of the item to be deleted
 * @param {boolean} [preventRender=false]   Do not re-render timeline if true
 *                                          (optimization for multiple delete)
 */
links.Timeline.prototype.deleteItem = function(index, preventRender) {
    if (index >= this.items.length) {
        throw "Cannot delete row, index out of range";
    }

    if (this.selection) {
        // adjust the selection
        if (this.selection.index == index) {
            // item to be deleted is selected
            this.unselectItem();
        }
        else if (this.selection.index > index) {
            // update selection index
            this.selection.index--;
        }
    }

    // actually delete the item and remove it from the DOM
    var item = this.items.splice(index, 1)[0];
    this.renderQueue.hide.push(item);

    // delete the row in the original data table
    if (this.data) {
        if (google && google.visualization &&
            this.data instanceof google.visualization.DataTable) {
            this.data.removeRow(index);
        }
        else if (links.Timeline.isArray(this.data)) {
            this.data.splice(index, 1);
        }
        else {
            throw "Cannot delete row from data, unknown data type";
        }
    }

    // prepare data for clustering, by filtering and sorting by type
    if (this.options.cluster) {
        this.clusterGenerator.updateData();
    }

    if (!preventRender) {
        this.render();
    }
};


/**
 * Delete all items
 */
links.Timeline.prototype.deleteAllItems = function() {
    this.unselectItem();

    // delete the loaded items
    this.clearItems();

    // delete the groups
    this.deleteGroups();

    // empty original data table
    if (this.data) {
        if (google && google.visualization &&
            this.data instanceof google.visualization.DataTable) {
            this.data.removeRows(0, this.data.getNumberOfRows());
        }
        else if (links.Timeline.isArray(this.data)) {
            this.data.splice(0, this.data.length);
        }
        else {
            throw "Cannot delete row from data, unknown data type";
        }
    }

    // prepare data for clustering, by filtering and sorting by type
    if (this.options.cluster) {
        this.clusterGenerator.updateData();
    }

    this.render();
};


/**
 * Find the group from a given height in the timeline
 * @param {Number} height   Height in the timeline
 * @return {Object | undefined} group   The group object, or undefined if out
 *                                      of range
 */
links.Timeline.prototype.getGroupFromHeight = function(height) {
    var i,
        group,
        groups = this.groups;

    if (groups.length) {
        if (this.options.axisOnTop) {
            for (i = groups.length - 1; i >= 0; i--) {
                group = groups[i];
                if (height > group.top) {
                    return group;
                }
            }
        }
        else {
            for (i = 0; i < groups.length; i++) {
                group = groups[i];
                if (height > group.top) {
                    return group;
                }
            }
        }

        return group; // return the last group
    }

    return undefined;
};

/**
 * @constructor links.Timeline.Item
 * @param {Object} data       Object containing parameters start, end
 *                            content, group, type, editable.
 * @param {Object} [options]  Options to set initial property values
 *                                {Number} top
 *                                {Number} left
 *                                {Number} width
 *                                {Number} height
 */
links.Timeline.Item = function (data, options) {
    if (data) {
        /* TODO: use parseJSONDate as soon as it is tested and working (in two directions)
         this.start = links.Timeline.parseJSONDate(data.start);
         this.end = links.Timeline.parseJSONDate(data.end);
         */
        this.start = data.start;
        this.end = data.end;
        this.content = data.content;
        this.className = data.className;
        this.editable = data.editable;
        this.group = data.group;
        this.type = data.type;
    }
    this.top = 0;
    this.left = 0;
    this.width = 0;
    this.height = 0;
    this.lineWidth = 0;
    this.dotWidth = 0;
    this.dotHeight = 0;

    this.rendered = false; // true when the item is draw in the Timeline DOM

    if (options) {
        // override the default properties
        for (var option in options) {
            if (options.hasOwnProperty(option)) {
                this[option] = options[option];
            }
        }
    }

};



/**
 * Reflow the Item: retrieve its actual size from the DOM
 * @return {boolean} resized    returns true if the axis is resized
 */
links.Timeline.Item.prototype.reflow = function () {
    // Should be implemented by sub-prototype
    return false;
};

/**
 * Append all image urls present in the items DOM to the provided array
 * @param {String[]} imageUrls
 */
links.Timeline.Item.prototype.getImageUrls = function (imageUrls) {
    if (this.dom) {
        links.imageloader.filterImageUrls(this.dom, imageUrls);
    }
};

/**
 * Select the item
 */
links.Timeline.Item.prototype.select = function () {
    // Should be implemented by sub-prototype
};

/**
 * Unselect the item
 */
links.Timeline.Item.prototype.unselect = function () {
    // Should be implemented by sub-prototype
};

/**
 * Creates the DOM for the item, depending on its type
 * @return {Element | undefined}
 */
links.Timeline.Item.prototype.createDOM = function () {
    // Should be implemented by sub-prototype
};

/**
 * Append the items DOM to the given HTML container. If items DOM does not yet
 * exist, it will be created first.
 * @param {Element} container
 */
links.Timeline.Item.prototype.showDOM = function (container) {
    // Should be implemented by sub-prototype
};

/**
 * Remove the items DOM from the current HTML container
 * @param {Element} container
 */
links.Timeline.Item.prototype.hideDOM = function (container) {
    // Should be implemented by sub-prototype
};

/**
 * Update the DOM of the item. This will update the content and the classes
 * of the item
 */
links.Timeline.Item.prototype.updateDOM = function () {
    // Should be implemented by sub-prototype
};

/**
 * Reposition the item, recalculate its left, top, and width, using the current
 * range of the timeline and the timeline options.
 * @param {links.Timeline} timeline
 */
links.Timeline.Item.prototype.updatePosition = function (timeline) {
    // Should be implemented by sub-prototype
};

/**
 * Check if the item is drawn in the timeline (i.e. the DOM of the item is
 * attached to the frame. You may also just request the parameter item.rendered
 * @return {boolean} rendered
 */
links.Timeline.Item.prototype.isRendered = function () {
    return this.rendered;
};

/**
 * Check if the item is located in the visible area of the timeline, and
 * not part of a cluster
 * @param {Date} start
 * @param {Date} end
 * @return {boolean} visible
 */
links.Timeline.Item.prototype.isVisible = function (start, end) {
    // Should be implemented by sub-prototype
    return false;
};

/**
 * Reposition the item
 * @param {Number} left
 * @param {Number} right
 */
links.Timeline.Item.prototype.setPosition = function (left, right) {
    // Should be implemented by sub-prototype
};

/**
 * Calculate the right position of the item
 * @param {links.Timeline} timeline
 * @return {Number} right
 */
links.Timeline.Item.prototype.getRight = function (timeline) {
    // Should be implemented by sub-prototype
    return 0;
};

/**
 * Calculate the width of the item
 * @param {links.Timeline} timeline
 * @return {Number} width
 */
links.Timeline.Item.prototype.getWidth = function (timeline) {
    // Should be implemented by sub-prototype
    return this.width || 0; // last rendered width
};


/**
 * @constructor links.Timeline.ItemBox
 * @extends links.Timeline.Item
 * @param {Object} data       Object containing parameters start, end
 *                            content, group, type, className, editable.
 * @param {Object} [options]  Options to set initial property values
 *                                {Number} top
 *                                {Number} left
 *                                {Number} width
 *                                {Number} height
 */
links.Timeline.ItemBox = function (data, options) {
    links.Timeline.Item.call(this, data, options);
};

links.Timeline.ItemBox.prototype = new links.Timeline.Item();

/**
 * Reflow the Item: retrieve its actual size from the DOM
 * @return {boolean} resized    returns true if the axis is resized
 * @override
 */
links.Timeline.ItemBox.prototype.reflow = function () {
    var dom = this.dom,
        dotHeight = dom.dot.offsetHeight,
        dotWidth = dom.dot.offsetWidth,
        lineWidth = dom.line.offsetWidth,
        resized = (
            (this.dotHeight != dotHeight) ||
                (this.dotWidth != dotWidth) ||
                (this.lineWidth != lineWidth)
            );

    this.dotHeight = dotHeight;
    this.dotWidth = dotWidth;
    this.lineWidth = lineWidth;

    return resized;
};

/**
 * Select the item
 * @override
 */
links.Timeline.ItemBox.prototype.select = function () {
    var dom = this.dom;
    links.Timeline.addClassName(dom, 'timeline-event-selected ui-state-active');
    links.Timeline.addClassName(dom.line, 'timeline-event-selected ui-state-active');
    links.Timeline.addClassName(dom.dot, 'timeline-event-selected ui-state-active');
};

/**
 * Unselect the item
 * @override
 */
links.Timeline.ItemBox.prototype.unselect = function () {
    var dom = this.dom;
    links.Timeline.removeClassName(dom, 'timeline-event-selected ui-state-active');
    links.Timeline.removeClassName(dom.line, 'timeline-event-selected ui-state-active');
    links.Timeline.removeClassName(dom.dot, 'timeline-event-selected ui-state-active');
};

/**
 * Creates the DOM for the item, depending on its type
 * @return {Element | undefined}
 * @override
 */
links.Timeline.ItemBox.prototype.createDOM = function () {
    // background box
    var divBox = document.createElement("DIV");
    divBox.style.position = "absolute";
    divBox.style.left = this.left + "px";
    divBox.style.top = this.top + "px";

    // contents box (inside the background box). used for making margins
    var divContent = document.createElement("DIV");
    divContent.className = "timeline-event-content";
    divContent.innerHTML = this.content;
    divBox.appendChild(divContent);

    // line to axis
    var divLine = document.createElement("DIV");
    divLine.style.position = "absolute";
    divLine.style.width = "0px";
    // important: the vertical line is added at the front of the list of elements,
    // so it will be drawn behind all boxes and ranges
    divBox.line = divLine;

    // dot on axis
    var divDot = document.createElement("DIV");
    divDot.style.position = "absolute";
    divDot.style.width  = "0px";
    divDot.style.height = "0px";
    divBox.dot = divDot;

    this.dom = divBox;
    this.updateDOM();

    return divBox;
};

/**
 * Append the items DOM to the given HTML container. If items DOM does not yet
 * exist, it will be created first.
 * @param {Element} container
 * @override
 */
links.Timeline.ItemBox.prototype.showDOM = function (container) {
    var dom = this.dom;
    if (!dom) {
        dom = this.createDOM();
    }

    if (dom.parentNode != container) {
        if (dom.parentNode) {
            // container is changed. remove from old container
            this.hideDOM();
        }

        // append to this container
        container.appendChild(dom);
        container.insertBefore(dom.line, container.firstChild);
        // Note: line must be added in front of the this,
        //       such that it stays below all this
        container.appendChild(dom.dot);
        this.rendered = true;
    }
};

/**
 * Remove the items DOM from the current HTML container, but keep the DOM in
 * memory
 * @override
 */
links.Timeline.ItemBox.prototype.hideDOM = function () {
    var dom = this.dom;
    if (dom) {
        if (dom.parentNode) {
            dom.parentNode.removeChild(dom);
        }
        if (dom.line && dom.line.parentNode) {
            dom.line.parentNode.removeChild(dom.line);
        }
        if (dom.dot && dom.dot.parentNode) {
            dom.dot.parentNode.removeChild(dom.dot);
        }
        this.rendered = false;
    }
};

/**
 * Update the DOM of the item. This will update the content and the classes
 * of the item
 * @override
 */
links.Timeline.ItemBox.prototype.updateDOM = function () {
    var divBox = this.dom;
    if (divBox) {
        var divLine = divBox.line;
        var divDot = divBox.dot;

        // update contents
        divBox.firstChild.innerHTML = this.content;

        // update class
        divBox.className = "timeline-event timeline-event-box ui-widget ui-state-default";
        divLine.className = "timeline-event timeline-event-line ui-widget ui-state-default";
        divDot.className  = "timeline-event timeline-event-dot ui-widget ui-state-default";

        if (this.isCluster) {
            links.Timeline.addClassName(divBox, 'timeline-event-cluster ui-widget-header');
            links.Timeline.addClassName(divLine, 'timeline-event-cluster ui-widget-header');
            links.Timeline.addClassName(divDot, 'timeline-event-cluster ui-widget-header');
        }

        // add item specific class name when provided
        if (this.className) {
            links.Timeline.addClassName(divBox, this.className);
            links.Timeline.addClassName(divLine, this.className);
            links.Timeline.addClassName(divDot, this.className);
        }

        // TODO: apply selected className?
    }
};

/**
 * Reposition the item, recalculate its left, top, and width, using the current
 * range of the timeline and the timeline options.
 * @param {links.Timeline} timeline
 * @override
 */
links.Timeline.ItemBox.prototype.updatePosition = function (timeline) {
    var dom = this.dom;
    if (dom) {
        var left = timeline.timeToScreen(this.start),
            axisOnTop = timeline.options.axisOnTop,
            axisTop = timeline.size.axis.top,
            axisHeight = timeline.size.axis.height,
            boxAlign = (timeline.options.box && timeline.options.box.align) ?
                timeline.options.box.align : undefined;

        dom.style.top = this.top + "px";
        if (boxAlign == 'right') {
            dom.style.left = (left - this.width) + "px";
        }
        else if (boxAlign == 'left') {
            dom.style.left = (left) + "px";
        }
        else { // default or 'center'
            dom.style.left = (left - this.width/2) + "px";
        }

        var line = dom.line;
        var dot = dom.dot;
        line.style.left = (left - this.lineWidth/2) + "px";
        dot.style.left = (left - this.dotWidth/2) + "px";
        if (axisOnTop) {
            line.style.top = axisHeight + "px";
            line.style.height = Math.max(this.top - axisHeight, 0) + "px";
            dot.style.top = (axisHeight - this.dotHeight/2) + "px";
        }
        else {
            line.style.top = (this.top + this.height) + "px";
            line.style.height = Math.max(axisTop - this.top - this.height, 0) + "px";
            dot.style.top = (axisTop - this.dotHeight/2) + "px";
        }
    }
};

/**
 * Check if the item is visible in the timeline, and not part of a cluster
 * @param {Date} start
 * @param {Date} end
 * @return {Boolean} visible
 * @override
 */
links.Timeline.ItemBox.prototype.isVisible = function (start, end) {
    if (this.cluster) {
        return false;
    }

    return (this.start > start) && (this.start < end);
};

/**
 * Reposition the item
 * @param {Number} left
 * @param {Number} right
 * @override
 */
links.Timeline.ItemBox.prototype.setPosition = function (left, right) {
    var dom = this.dom;

    dom.style.left = (left - this.width / 2) + "px";
    dom.line.style.left = (left - this.lineWidth / 2) + "px";
    dom.dot.style.left = (left - this.dotWidth / 2) + "px";

    if (this.group) {
        this.top = this.group.top;
        dom.style.top = this.top + 'px';
    }
};

/**
 * Calculate the right position of the item
 * @param {links.Timeline} timeline
 * @return {Number} right
 * @override
 */
links.Timeline.ItemBox.prototype.getRight = function (timeline) {
    var boxAlign = (timeline.options.box && timeline.options.box.align) ?
        timeline.options.box.align : undefined;

    var left = timeline.timeToScreen(this.start);
    var right;
    if (boxAlign == 'right') {
        right = left;
    }
    else if (boxAlign == 'left') {
        right = (left + this.width);
    }
    else { // default or 'center'
        right = (left + this.width / 2);
    }

    return right;
};

/**
 * @constructor links.Timeline.ItemRange
 * @extends links.Timeline.Item
 * @param {Object} data       Object containing parameters start, end
 *                            content, group, type, className, editable.
 * @param {Object} [options]  Options to set initial property values
 *                                {Number} top
 *                                {Number} left
 *                                {Number} width
 *                                {Number} height
 */
links.Timeline.ItemRange = function (data, options) {
    links.Timeline.Item.call(this, data, options);
};

links.Timeline.ItemRange.prototype = new links.Timeline.Item();

/**
 * Select the item
 * @override
 */
links.Timeline.ItemRange.prototype.select = function () {
    var dom = this.dom;
    links.Timeline.addClassName(dom, 'timeline-event-selected ui-state-active');
};

/**
 * Unselect the item
 * @override
 */
links.Timeline.ItemRange.prototype.unselect = function () {
    var dom = this.dom;
    links.Timeline.removeClassName(dom, 'timeline-event-selected ui-state-active');
};

/**
 * Creates the DOM for the item, depending on its type
 * @return {Element | undefined}
 * @override
 */
links.Timeline.ItemRange.prototype.createDOM = function () {
    // background box
    var divBox = document.createElement("DIV");
    divBox.style.position = "absolute";

    // contents box
    var divContent = document.createElement("DIV");
    divContent.className = "timeline-event-content";
    divBox.appendChild(divContent);

    this.dom = divBox;
    this.updateDOM();

    return divBox;
};

/**
 * Append the items DOM to the given HTML container. If items DOM does not yet
 * exist, it will be created first.
 * @param {Element} container
 * @override
 */
links.Timeline.ItemRange.prototype.showDOM = function (container) {
    var dom = this.dom;
    if (!dom) {
        dom = this.createDOM();
    }

    if (dom.parentNode != container) {
        if (dom.parentNode) {
            // container changed. remove the item from the old container
            this.hideDOM();
        }

        // append to the new container
        container.appendChild(dom);
        this.rendered = true;
    }
};

/**
 * Remove the items DOM from the current HTML container
 * The DOM will be kept in memory
 * @override
 */
links.Timeline.ItemRange.prototype.hideDOM = function () {
    var dom = this.dom;
    if (dom) {
        if (dom.parentNode) {
            dom.parentNode.removeChild(dom);
        }
        this.rendered = false;
    }
};

/**
 * Update the DOM of the item. This will update the content and the classes
 * of the item
 * @override
 */
links.Timeline.ItemRange.prototype.updateDOM = function () {
    var divBox = this.dom;
    if (divBox) {
        // update contents
        divBox.firstChild.innerHTML = this.content;

        // update class
        divBox.className = "timeline-event timeline-event-range ui-widget ui-state-default";

        if (this.isCluster) {
            links.Timeline.addClassName(divBox, 'timeline-event-cluster ui-widget-header');
        }

        // add item specific class name when provided
        if (this.className) {
            links.Timeline.addClassName(divBox, this.className);
        }

        // TODO: apply selected className?
    }
};

/**
 * Reposition the item, recalculate its left, top, and width, using the current
 * range of the timeline and the timeline options. *
 * @param {links.Timeline} timeline
 * @override
 */
links.Timeline.ItemRange.prototype.updatePosition = function (timeline) {
    var dom = this.dom;
    if (dom) {
        var contentWidth = timeline.size.contentWidth,
            left = timeline.timeToScreen(this.start),
            right = timeline.timeToScreen(this.end);

        // limit the width of the this, as browsers cannot draw very wide divs
        if (left < -contentWidth) {
            left = -contentWidth;
        }
        if (right > 2 * contentWidth) {
            right = 2 * contentWidth;
        }

        dom.style.top = this.top + "px";
        dom.style.left = left + "px";
        //dom.style.width = Math.max(right - left - 2 * this.borderWidth, 1) + "px"; // TODO: borderWidth
        dom.style.width = Math.max(right - left, 1) + "px";
    }
};

/**
 * Check if the item is visible in the timeline, and not part of a cluster
 * @param {Number} start
 * @param {Number} end
 * @return {boolean} visible
 * @override
 */
links.Timeline.ItemRange.prototype.isVisible = function (start, end) {
    if (this.cluster) {
        return false;
    }

    return (this.end > start)
        && (this.start < end);
};

/**
 * Reposition the item
 * @param {Number} left
 * @param {Number} right
 * @override
 */
links.Timeline.ItemRange.prototype.setPosition = function (left, right) {
    var dom = this.dom;

    dom.style.left = left + 'px';
    dom.style.width = (right - left) + 'px';

    if (this.group) {
        this.top = this.group.top;
        dom.style.top = this.top + 'px';
    }
};

/**
 * Calculate the right position of the item
 * @param {links.Timeline} timeline
 * @return {Number} right
 * @override
 */
links.Timeline.ItemRange.prototype.getRight = function (timeline) {
    return timeline.timeToScreen(this.end);
};

/**
 * Calculate the width of the item
 * @param {links.Timeline} timeline
 * @return {Number} width
 * @override
 */
links.Timeline.ItemRange.prototype.getWidth = function (timeline) {
    return timeline.timeToScreen(this.end) - timeline.timeToScreen(this.start);
};

/**
 * @constructor links.Timeline.ItemDot
 * @extends links.Timeline.Item
 * @param {Object} data       Object containing parameters start, end
 *                            content, group, type, className, editable.
 * @param {Object} [options]  Options to set initial property values
 *                                {Number} top
 *                                {Number} left
 *                                {Number} width
 *                                {Number} height
 */
links.Timeline.ItemDot = function (data, options) {
    links.Timeline.Item.call(this, data, options);
};

links.Timeline.ItemDot.prototype = new links.Timeline.Item();

/**
 * Reflow the Item: retrieve its actual size from the DOM
 * @return {boolean} resized    returns true if the axis is resized
 * @override
 */
links.Timeline.ItemDot.prototype.reflow = function () {
    var dom = this.dom,
        dotHeight = dom.dot.offsetHeight,
        dotWidth = dom.dot.offsetWidth,
        contentHeight = dom.content.offsetHeight,
        resized = (
            (this.dotHeight != dotHeight) ||
                (this.dotWidth != dotWidth) ||
                (this.contentHeight != contentHeight)
            );

    this.dotHeight = dotHeight;
    this.dotWidth = dotWidth;
    this.contentHeight = contentHeight;

    return resized;
};

/**
 * Select the item
 * @override
 */
links.Timeline.ItemDot.prototype.select = function () {
    var dom = this.dom;
    links.Timeline.addClassName(dom, 'timeline-event-selected ui-state-active');
};

/**
 * Unselect the item
 * @override
 */
links.Timeline.ItemDot.prototype.unselect = function () {
    var dom = this.dom;
    links.Timeline.removeClassName(dom, 'timeline-event-selected ui-state-active');
};

/**
 * Creates the DOM for the item, depending on its type
 * @return {Element | undefined}
 * @override
 */
links.Timeline.ItemDot.prototype.createDOM = function () {
    // background box
    var divBox = document.createElement("DIV");
    divBox.style.position = "absolute";

    // contents box, right from the dot
    var divContent = document.createElement("DIV");
    divContent.className = "timeline-event-content";
    divBox.appendChild(divContent);

    // dot at start
    var divDot = document.createElement("DIV");
    divDot.style.position = "absolute";
    divDot.style.width = "0px";
    divDot.style.height = "0px";
    divBox.appendChild(divDot);

    divBox.content = divContent;
    divBox.dot = divDot;

    this.dom = divBox;
    this.updateDOM();

    return divBox;
};

/**
 * Append the items DOM to the given HTML container. If items DOM does not yet
 * exist, it will be created first.
 * @param {Element} container
 * @override
 */
links.Timeline.ItemDot.prototype.showDOM = function (container) {
    var dom = this.dom;
    if (!dom) {
        dom = this.createDOM();
    }

    if (dom.parentNode != container) {
        if (dom.parentNode) {
            // container changed. remove it from old container first
            this.hideDOM();
        }

        // append to container
        container.appendChild(dom);
        this.rendered = true;
    }
};

/**
 * Remove the items DOM from the current HTML container
 * @override
 */
links.Timeline.ItemDot.prototype.hideDOM = function () {
    var dom = this.dom;
    if (dom) {
        if (dom.parentNode) {
            dom.parentNode.removeChild(dom);
        }
        this.rendered = false;
    }
};

/**
 * Update the DOM of the item. This will update the content and the classes
 * of the item
 * @override
 */
links.Timeline.ItemDot.prototype.updateDOM = function () {
    if (this.dom) {
        var divBox = this.dom;
        var divDot = divBox.dot;

        // update contents
        divBox.firstChild.innerHTML = this.content;

        // update classes
        divBox.className = "timeline-event-dot-container";
        divDot.className  = "timeline-event timeline-event-dot ui-widget ui-state-default";

        if (this.isCluster) {
            links.Timeline.addClassName(divBox, 'timeline-event-cluster ui-widget-header');
            links.Timeline.addClassName(divDot, 'timeline-event-cluster ui-widget-header');
        }

        // add item specific class name when provided
        if (this.className) {
            links.Timeline.addClassName(divBox, this.className);
            links.Timeline.addClassName(divDot, this.className);
        }

        // TODO: apply selected className?
    }
};

/**
 * Reposition the item, recalculate its left, top, and width, using the current
 * range of the timeline and the timeline options. *
 * @param {links.Timeline} timeline
 * @override
 */
links.Timeline.ItemDot.prototype.updatePosition = function (timeline) {
    var dom = this.dom;
    if (dom) {
        var left = timeline.timeToScreen(this.start);

        dom.style.top = this.top + "px";
        dom.style.left = (left - this.dotWidth / 2) + "px";

        dom.content.style.marginLeft = (1.5 * this.dotWidth) + "px";
        //dom.content.style.marginRight = (0.5 * this.dotWidth) + "px"; // TODO
        dom.dot.style.top = ((this.height - this.dotHeight) / 2) + "px";
    }
};

/**
 * Check if the item is visible in the timeline, and not part of a cluster.
 * @param {Date} start
 * @param {Date} end
 * @return {boolean} visible
 * @override
 */
links.Timeline.ItemDot.prototype.isVisible = function (start, end) {
    if (this.cluster) {
        return false;
    }

    return (this.start > start)
        && (this.start < end);
};

/**
 * Reposition the item
 * @param {Number} left
 * @param {Number} right
 * @override
 */
links.Timeline.ItemDot.prototype.setPosition = function (left, right) {
    var dom = this.dom;

    dom.style.left = (left - this.dotWidth / 2) + "px";

    if (this.group) {
        this.top = this.group.top;
        dom.style.top = this.top + 'px';
    }
};

/**
 * Calculate the right position of the item
 * @param {links.Timeline} timeline
 * @return {Number} right
 * @override
 */
links.Timeline.ItemDot.prototype.getRight = function (timeline) {
    return timeline.timeToScreen(this.start) + this.width;
};

/**
 * Retrieve the properties of an item.
 * @param {Number} index
 * @return {Object} properties  Object containing item properties:<br>
 *                              {Date} start (required),
 *                              {Date} end (optional),
 *                              {String} content (required),
 *                              {String} group (optional),
 *                              {String} className (optional)
 *                              {boolean} editable (optional)
 *                              {String} type (optional)
 */
links.Timeline.prototype.getItem = function (index) {
    if (index >= this.items.length) {
        throw "Cannot get item, index out of range";
    }

    var item = this.items[index];

    var properties = {};
    properties.start = new Date(item.start.valueOf());
    if (item.end) {
        properties.end = new Date(item.end.valueOf());
    }
    properties.content = item.content;
    if (item.group) {
        properties.group = this.getGroupName(item.group);
    }
    if ('className' in item) {
        properties.className = this.getGroupName(item.className);
    }
    if (item.hasOwnProperty('editable') && (typeof item.editable != 'undefined')) {
        properties.editable = item.editable;
    }
    if (item.type) {
        properties.type = item.type;
    }

    return properties;
};

/**
 * Add a new item.
 * @param {Object} itemData     Object containing item properties:<br>
 *                              {Date} start (required),
 *                              {Date} end (optional),
 *                              {String} content (required),
 *                              {String} group (optional)
 *                              {String} className (optional)
 *                              {Boolean} editable (optional)
 *                              {String} type (optional)
 * @param {boolean} [preventRender=false]   Do not re-render timeline if true
 */
links.Timeline.prototype.addItem = function (itemData, preventRender) {
    var itemsData = [
        itemData
    ];

    this.addItems(itemsData, preventRender);
};

/**
 * Add new items.
 * @param {Array} itemsData An array containing Objects.
 *                          The objects must have the following parameters:
 *                            {Date} start,
 *                            {Date} end,
 *                            {String} content with text or HTML code,
 *                            {String} group (optional)
 *                            {String} className (optional)
 *                            {String} editable (optional)
 *                            {String} type (optional)
 * @param {boolean} [preventRender=false]   Do not re-render timeline if true
 */
links.Timeline.prototype.addItems = function (itemsData, preventRender) {
    var timeline = this,
        items = this.items;

    // append the items
    itemsData.forEach(function (itemData) {
        var index = items.length;
        items.push(timeline.createItem(itemData));
        timeline.updateData(index, itemData);

        // note: there is no need to add the item to the renderQueue, that
        // will be done when this.render() is executed and all items are
        // filtered again.
    });

    // prepare data for clustering, by filtering and sorting by type
    if (this.options.cluster) {
        this.clusterGenerator.updateData();
    }

    if (!preventRender) {
        this.render({
            animate: false
        });
    }
};

/**
 * Create an item object, containing all needed parameters
 * @param {Object} itemData  Object containing parameters start, end
 *                           content, group.
 * @return {Object} item
 */
links.Timeline.prototype.createItem = function(itemData) {
    var type = itemData.type || (itemData.end ? 'range' : this.options.style);
    var data = {
        start: itemData.start,
        end: itemData.end,
        content: itemData.content,
        className: itemData.className,
        editable: itemData.editable,
        group: this.getGroup(itemData.group),
        type: type
    };
    // TODO: optimize this, when creating an item, all data is copied twice...

    // TODO: is initialTop needed?
    var initialTop,
        options = this.options;
    if (options.axisOnTop) {
        initialTop = this.size.axis.height + options.eventMarginAxis + options.eventMargin / 2;
    }
    else {
        initialTop = this.size.contentHeight - options.eventMarginAxis - options.eventMargin / 2;
    }

    if (type in this.itemTypes) {
        return new this.itemTypes[type](data, {'top': initialTop})
    }

    console.log('ERROR: Unknown event style "' + type + '"');
    return new links.Timeline.Item(data, {
        'top': initialTop
    });
};

/**
 * Edit an item
 * @param {Number} index
 * @param {Object} itemData     Object containing item properties:<br>
 *                              {Date} start (required),
 *                              {Date} end (optional),
 *                              {String} content (required),
 *                              {String} group (optional)
 * @param {boolean} [preventRender=false]   Do not re-render timeline if true
 */
links.Timeline.prototype.changeItem = function (index, itemData, preventRender) {
    var oldItem = this.items[index];
    if (!oldItem) {
        throw "Cannot change item, index out of range";
    }

    // replace item, merge the changes
    var newItem = this.createItem({
        'start':     itemData.hasOwnProperty('start') ?     itemData.start :     oldItem.start,
        'end':       itemData.hasOwnProperty('end') ?       itemData.end :       oldItem.end,
        'content':   itemData.hasOwnProperty('content') ?   itemData.content :   oldItem.content,
        'group':     itemData.hasOwnProperty('group') ?     itemData.group :     this.getGroupName(oldItem.group),
        'className': itemData.hasOwnProperty('className') ? itemData.className : oldItem.className,
        'editable':  itemData.hasOwnProperty('editable') ?  itemData.editable :  oldItem.editable,
        'type':      itemData.hasOwnProperty('type') ?      itemData.type :      oldItem.type
    });
    this.items[index] = newItem;

    // append the changes to the render queue
    this.renderQueue.hide.push(oldItem);
    this.renderQueue.show.push(newItem);

    // update the original data table
    this.updateData(index, itemData);

    // prepare data for clustering, by filtering and sorting by type
    if (this.options.cluster) {
        this.clusterGenerator.updateData();
    }

    if (!preventRender) {
        // redraw timeline
        this.render({
            animate: false
        });

        if (this.selection && this.selection.index == index) {
            newItem.select();
        }
    }
};

/**
 * Delete all groups
 */
links.Timeline.prototype.deleteGroups = function () {
    this.groups = [];
    this.groupIndexes = {};
};


/**
 * Get a group by the group name. When the group does not exist,
 * it will be created.
 * @param {String} groupName   the name of the group
 * @return {Object} groupObject
 */
links.Timeline.prototype.getGroup = function (groupName) {
    var groups = this.groups,
        groupIndexes = this.groupIndexes,
        groupObj = undefined;

    var groupIndex = groupIndexes[groupName];
    if (groupIndex == undefined && groupName != undefined) { // not null or undefined
        groupObj = {
            'content': groupName,
            'labelTop': 0,
            'lineTop': 0
            // note: this object will lateron get addition information, 
            //       such as height and width of the group         
        };
        groups.push(groupObj);
        // sort the groups
        groups = groups.sort(function (a, b) {
            if (a.content > b.content) {
                return 1;
            }
            if (a.content < b.content) {
                return -1;
            }
            return 0;
        });

        // rebuilt the groupIndexes
        for (var i = 0, iMax = groups.length; i < iMax; i++) {
            groupIndexes[groups[i].content] = i;
        }
    }
    else {
        groupObj = groups[groupIndex];
    }

    return groupObj;
};

/**
 * Get the group name from a group object.
 * @param {Object} groupObj
 * @return {String} groupName   the name of the group, or undefined when group
 *                              was not provided
 */
links.Timeline.prototype.getGroupName = function (groupObj) {
    return groupObj ? groupObj.content : undefined;
};

/**
 * Cancel a change item
 * This method can be called insed an event listener which catches the "change"
 * event. The changed event position will be undone.
 */
links.Timeline.prototype.cancelChange = function () {
    this.applyChange = false;
};

/**
 * Cancel deletion of an item
 * This method can be called insed an event listener which catches the "delete"
 * event. Deletion of the event will be undone.
 */
links.Timeline.prototype.cancelDelete = function () {
    this.applyDelete = false;
};


/**
 * Cancel creation of a new item
 * This method can be called insed an event listener which catches the "new"
 * event. Creation of the new the event will be undone.
 */
links.Timeline.prototype.cancelAdd = function () {
    this.applyAdd = false;
};


/**
 * Select an event. The visible chart range will be moved such that the selected
 * event is placed in the middle.
 * For example selection = [{row: 5}];
 * @param {Array} selection   An array with a column row, containing the row
 *                           number (the id) of the event to be selected.
 * @return {boolean}         true if selection is succesfully set, else false.
 */
links.Timeline.prototype.setSelection = function(selection) {
    if (selection != undefined && selection.length > 0) {
        if (selection[0].row != undefined) {
            var index = selection[0].row;
            if (this.items[index]) {
                var item = this.items[index];
                this.selectItem(index);

                // move the visible chart range to the selected event.
                var start = item.start;
                var end = item.end;
                var middle; // number
                if (end != undefined) {
                    middle = (end.valueOf() + start.valueOf()) / 2;
                } else {
                    middle = start.valueOf();
                }
                var diff = (this.end.valueOf() - this.start.valueOf()),
                    newStart = new Date(middle - diff/2),
                    newEnd = new Date(middle + diff/2);

                this.setVisibleChartRange(newStart, newEnd);

                return true;
            }
        }
    }
    else {
        // unselect current selection
        this.unselectItem();
    }
    return false;
};

/**
 * Retrieve the currently selected event
 * @return {Array} sel  An array with a column row, containing the row number
 *                      of the selected event. If there is no selection, an
 *                      empty array is returned.
 */
links.Timeline.prototype.getSelection = function() {
    var sel = [];
    if (this.selection) {
        sel.push({"row": this.selection.index});
    }
    return sel;
};


/**
 * Select an item by its index
 * @param {Number} index
 */
links.Timeline.prototype.selectItem = function(index) {
    this.unselectItem();

    this.selection = undefined;

    if (this.items[index] != undefined) {
        var item = this.items[index],
            domItem = item.dom;

        this.selection = {
            'index': index
        };

        if (item && item.dom) {
            // TODO: move adjusting the domItem to the item itself
            if (this.isEditable(item)) {
                item.dom.style.cursor = 'move';
            }
            item.select();
        }
        this.repaintDeleteButton();
        this.repaintDragAreas();
    }
};

/**
 * Check if an item is currently selected
 * @param {Number} index
 * @return {boolean} true if row is selected, else false
 */
links.Timeline.prototype.isSelected = function (index) {
    return (this.selection && this.selection.index == index);
};

/**
 * Unselect the currently selected event (if any)
 */
links.Timeline.prototype.unselectItem = function() {
    if (this.selection) {
        var item = this.items[this.selection.index];

        if (item && item.dom) {
            var domItem = item.dom;
            domItem.style.cursor = '';
            item.unselect();
        }

        this.selection = undefined;
        this.repaintDeleteButton();
        this.repaintDragAreas();
    }
};


/**
 * Stack the items such that they don't overlap. The items will have a minimal
 * distance equal to options.eventMargin.
 * @param {boolean | undefined} animate    if animate is true, the items are
 *                                         moved to their new position animated
 *                                         defaults to false.
 */
links.Timeline.prototype.stackItems = function(animate) {
    if (this.groups.length > 0) {
        // under this conditions we refuse to stack the events
        // TODO: implement support for stacking items per group
        return;
    }

    if (animate == undefined) {
        animate = false;
    }

    // calculate the order and final stack position of the items
    var stack = this.stack;
    if (!stack) {
        stack = {};
        this.stack = stack;
    }
    stack.sortedItems = this.stackOrder(this.renderedItems);
    stack.finalItems = this.stackCalculateFinal(stack.sortedItems);

    if (animate || stack.timer) {
        // move animated to the final positions
        var timeline = this;
        var step = function () {
            var arrived = timeline.stackMoveOneStep(stack.sortedItems,
                stack.finalItems);

            timeline.repaint();

            if (!arrived) {
                stack.timer = setTimeout(step, 30);
            }
            else {
                delete stack.timer;
            }
        };

        if (!stack.timer) {
            stack.timer = setTimeout(step, 30);
        }
    }
    else {
        // move immediately to the final positions
        this.stackMoveToFinal(stack.sortedItems, stack.finalItems);
    }
};

/**
 * Cancel any running animation
 */
links.Timeline.prototype.stackCancelAnimation = function() {
    if (this.stack && this.stack.timer) {
        clearTimeout(this.stack.timer);
        delete this.stack.timer;
    }
};


/**
 * Order the items in the array this.items. The default order is determined via:
 * - Ranges go before boxes and dots.
 * - The item with the oldest start time goes first
 * If a custom function has been provided via the stackorder option, then this will be used.
 * @param {Array} items        Array with items
 * @return {Array} sortedItems Array with sorted items
 */
links.Timeline.prototype.stackOrder = function(items) {
    // TODO: store the sorted items, to have less work later on
    var sortedItems = items.concat([]);

    //if a customer stack order function exists, use it. 
    var f = this.options.customStackOrder && (typeof this.options.customStackOrder === 'function') ? this.options.customStackOrder : function (a, b)
    {
        if ((a instanceof links.Timeline.ItemRange) &&
            !(b instanceof links.Timeline.ItemRange)) {
            return -1;
        }

        if (!(a instanceof links.Timeline.ItemRange) &&
            (b instanceof links.Timeline.ItemRange)) {
            return 1;
        }

        return (a.left - b.left);
    };

    sortedItems.sort(f);

    return sortedItems;
};

/**
 * Adjust vertical positions of the events such that they don't overlap each
 * other.
 * @param {timeline.Item[]} items
 * @return {Object[]} finalItems
 */
links.Timeline.prototype.stackCalculateFinal = function(items) {
    var i,
        iMax,
        size = this.size,
        axisTop = size.axis.top,
        axisHeight = size.axis.height,
        options = this.options,
        axisOnTop = options.axisOnTop,
        eventMargin = options.eventMargin,
        eventMarginAxis = options.eventMarginAxis,
        finalItems = [];

    // initialize final positions
    for (i = 0, iMax = items.length; i < iMax; i++) {
        var item = items[i],
            top,
            bottom,
            height = item.height,
            width = item.getWidth(this),
            right = item.getRight(this),
            left = right - width;

        if (axisOnTop) {
            top = axisHeight + eventMarginAxis + eventMargin / 2;
        }
        else {
            top = axisTop - height - eventMarginAxis - eventMargin / 2;
        }
        bottom = top + height;

        finalItems[i] = {
            'left': left,
            'top': top,
            'right': right,
            'bottom': bottom,
            'height': height,
            'item': item
        };
    }

    if (this.options.stackEvents) {
        // calculate new, non-overlapping positions
        //var items = sortedItems;
        for (i = 0, iMax = finalItems.length; i < iMax; i++) {
            //for (var i = finalItems.length - 1; i >= 0; i--) {
            var finalItem = finalItems[i];
            var collidingItem = null;
            do {
                // TODO: optimize checking for overlap. when there is a gap without items,
                //  you only need to check for items from the next item on, not from zero
                collidingItem = this.stackItemsCheckOverlap(finalItems, i, 0, i-1);
                if (collidingItem != null) {
                    // There is a collision. Reposition the event above the colliding element
                    if (axisOnTop) {
                        finalItem.top = collidingItem.top + collidingItem.height + eventMargin;
                    }
                    else {
                        finalItem.top = collidingItem.top - finalItem.height - eventMargin;
                    }
                    finalItem.bottom = finalItem.top + finalItem.height;
                }
            } while (collidingItem);
        }
    }

    return finalItems;
};


/**
 * Move the events one step in the direction of their final positions
 * @param {Array} currentItems   Array with the real items and their current
 *                               positions
 * @param {Array} finalItems     Array with objects containing the final
 *                               positions of the items
 * @return {boolean} arrived     True if all items have reached their final
 *                               location, else false
 */
links.Timeline.prototype.stackMoveOneStep = function(currentItems, finalItems) {
    var arrived = true;

    // apply new positions animated
    for (i = 0, iMax = finalItems.length; i < iMax; i++) {
        var finalItem = finalItems[i],
            item = finalItem.item;

        var topNow = parseInt(item.top);
        var topFinal = parseInt(finalItem.top);
        var diff = (topFinal - topNow);
        if (diff) {
            var step = (topFinal == topNow) ? 0 : ((topFinal > topNow) ? 1 : -1);
            if (Math.abs(diff) > 4) step = diff / 4;
            var topNew = parseInt(topNow + step);

            if (topNew != topFinal) {
                arrived = false;
            }

            item.top = topNew;
            item.bottom = item.top + item.height;
        }
        else {
            item.top = finalItem.top;
            item.bottom = finalItem.bottom;
        }

        item.left = finalItem.left;
        item.right = finalItem.right;
    }

    return arrived;
};



/**
 * Move the events from their current position to the final position
 * @param {Array} currentItems   Array with the real items and their current
 *                               positions
 * @param {Array} finalItems     Array with objects containing the final
 *                               positions of the items
 */
links.Timeline.prototype.stackMoveToFinal = function(currentItems, finalItems) {
    // Put the events directly at there final position
    for (i = 0, iMax = finalItems.length; i < iMax; i++) {
        var finalItem = finalItems[i],
            current = finalItem.item;

        current.left = finalItem.left;
        current.top = finalItem.top;
        current.right = finalItem.right;
        current.bottom = finalItem.bottom;
    }
};



/**
 * Check if the destiny position of given item overlaps with any
 * of the other items from index itemStart to itemEnd.
 * @param {Array} items      Array with items
 * @param {int}  itemIndex   Number of the item to be checked for overlap
 * @param {int}  itemStart   First item to be checked.
 * @param {int}  itemEnd     Last item to be checked.
 * @return {Object}          colliding item, or undefined when no collisions
 */
links.Timeline.prototype.stackItemsCheckOverlap = function(items, itemIndex,
                                                           itemStart, itemEnd) {
    var eventMargin = this.options.eventMargin,
        collision = this.collision;

    // we loop from end to start, as we suppose that the chance of a 
    // collision is larger for items at the end, so check these first.
    var item1 = items[itemIndex];
    for (var i = itemEnd; i >= itemStart; i--) {
        var item2 = items[i];
        if (collision(item1, item2, eventMargin)) {
            if (i != itemIndex) {
                return item2;
            }
        }
    }

    return undefined;
};

/**
 * Test if the two provided items collide
 * The items must have parameters left, right, top, and bottom.
 * @param {Element} item1       The first item
 * @param {Element} item2       The second item
 * @param {Number}              margin  A minimum required margin. Optional.
 *                              If margin is provided, the two items will be
 *                              marked colliding when they overlap or
 *                              when the margin between the two is smaller than
 *                              the requested margin.
 * @return {boolean}            true if item1 and item2 collide, else false
 */
links.Timeline.prototype.collision = function(item1, item2, margin) {
    // set margin if not specified 
    if (margin == undefined) {
        margin = 0;
    }

    // calculate if there is overlap (collision)
    return (item1.left - margin < item2.right &&
        item1.right + margin > item2.left &&
        item1.top - margin < item2.bottom &&
        item1.bottom + margin > item2.top);
};


/**
 * fire an event
 * @param {String} event   The name of an event, for example "rangechange" or "edit"
 */
links.Timeline.prototype.trigger = function (event) {
    // built up properties
    var properties = null;
    switch (event) {
        case 'rangechange':
        case 'rangechanged':
            properties = {
                'start': new Date(this.start.valueOf()),
                'end': new Date(this.end.valueOf())
            };
            break;

        case 'timechange':
        case 'timechanged':
            properties = {
                'time': new Date(this.customTime.valueOf())
            };
            break;
    }

    // trigger the links event bus
    links.events.trigger(this, event, properties);

    // trigger the google event bus
    if (google && google.visualization) {
        google.visualization.events.trigger(this, event, properties);
    }
};


/**
 * Cluster the events
 */
links.Timeline.prototype.clusterItems = function () {
    if (!this.options.cluster) {
        return;
    }

    var clusters = this.clusterGenerator.getClusters(this.conversion.factor);
    if (this.clusters != clusters) {
        // cluster level changed
        var queue = this.renderQueue;

        // remove the old clusters from the scene
        if (this.clusters) {
            this.clusters.forEach(function (cluster) {
                queue.hide.push(cluster);

                // unlink the items
                cluster.items.forEach(function (item) {
                    item.cluster = undefined;
                });
            });
        }

        // append the new clusters
        clusters.forEach(function (cluster) {
            // don't add to the queue.show here, will be done in .filterItems()

            // link all items to the cluster
            cluster.items.forEach(function (item) {
                item.cluster = cluster;
            });
        });

        this.clusters = clusters;
    }
};

/**
 * Filter the visible events
 */
links.Timeline.prototype.filterItems = function () {
    var queue = this.renderQueue,
        window = (this.end - this.start),
        start = new Date(this.start.valueOf() - window),
        end = new Date(this.end.valueOf() + window);

    function filter (arr) {
        arr.forEach(function (item) {
            var rendered = item.rendered;
            var visible = item.isVisible(start, end);
            if (rendered != visible) {
                if (rendered) {
                    queue.hide.push(item); // item is rendered but no longer visible
                }
                if (visible && (queue.show.indexOf(item) == -1)) {
                    queue.show.push(item); // item is visible but neither rendered nor queued up to be rendered
                }
            }
        });
    }

    // filter all items and all clusters
    filter(this.items);
    if (this.clusters) {
        filter(this.clusters);
    }
};

/** ------------------------------------------------------------------------ **/

/**
 * @constructor links.Timeline.ClusterGenerator
 * Generator which creates clusters of items, based on the visible range in
 * the Timeline. There is a set of cluster levels which is cached.
 * @param {links.Timeline} timeline
 */
links.Timeline.ClusterGenerator = function (timeline) {
    this.timeline = timeline;
    this.clear();
};

/**
 * Clear all cached clusters and data, and initialize all variables
 */
links.Timeline.ClusterGenerator.prototype.clear = function () {
    // cache containing created clusters for each cluster level
    this.items = [];
    this.groups = {};
    this.clearCache();
};

/**
 * Clear the cached clusters
 */
links.Timeline.ClusterGenerator.prototype.clearCache = function () {
    // cache containing created clusters for each cluster level
    this.cache = {};
    this.cacheLevel = -1;
    this.cache[this.cacheLevel] = [];
};

/**
 * Set the items to be clustered.
 * This will clear cached clusters.
 * @param {Item[]} items
 * @param {Object} [options]  Available options:
 *                            {boolean} applyOnChangedLevel
 *                                If true (default), the changed data is applied
 *                                as soon the cluster level changes. If false,
 *                                The changed data is applied immediately
 */
links.Timeline.ClusterGenerator.prototype.setData = function (items, options) {
    this.items = items || [];
    this.dataChanged = true;
    this.applyOnChangedLevel = true;
    if (options && options.applyOnChangedLevel) {
        this.applyOnChangedLevel = options.applyOnChangedLevel;
    }
    // console.log('clustergenerator setData applyOnChangedLevel=' + this.applyOnChangedLevel); // TODO: cleanup
};

/**
 * Update the current data set: clear cache, and recalculate the clustering for
 * the current level
 */
links.Timeline.ClusterGenerator.prototype.updateData = function () {
    this.dataChanged = true;
    this.applyOnChangedLevel = false;
};

/**
 * Filter the items per group.
 * @private
 */
links.Timeline.ClusterGenerator.prototype.filterData = function () {
    // filter per group
    var items = this.items || [];
    var groups = {};
    this.groups = groups;

    // split the items per group
    items.forEach(function (item) {
        // put the item in the correct group
        var groupName = item.group ? item.group.content : '';
        var group = groups[groupName];
        if (!group) {
            group = [];
            groups[groupName] = group;
        }
        group.push(item);

        // calculate the center of the item
        if (item.start) {
            if (item.end) {
                // range
                item.center = (item.start.valueOf() + item.end.valueOf()) / 2;
            }
            else {
                // box, dot
                item.center = item.start.valueOf();
            }
        }
    });

    // sort the items per group
    for (var groupName in groups) {
        if (groups.hasOwnProperty(groupName)) {
            groups[groupName].sort(function (a, b) {
                return (a.center - b.center);
            });
        }
    }

    this.dataChanged = false;
};

/**
 * Cluster the events which are too close together
 * @param {Number} scale     The scale of the current window,
 *                           defined as (windowWidth / (endDate - startDate))
 * @return {Item[]} clusters
 */
links.Timeline.ClusterGenerator.prototype.getClusters = function (scale) {
    var level = -1,
        granularity = 2, // TODO: what granularity is needed for the cluster levels?
        timeWindow = 0,  // milliseconds
        maxItems = 5;    // TODO: do not hard code maxItems

    if (scale > 0) {
        level = Math.round(Math.log(100 / scale) / Math.log(granularity));
        timeWindow = Math.pow(granularity, level);

        // groups must have a larger time window, as the items will not be stacked
        if (this.timeline.groups && this.timeline.groups.length) {
            timeWindow *= 4;
        }
    }

    // clear the cache when and re-filter the data when needed.
    if (this.dataChanged) {
        var levelChanged = (level != this.cacheLevel);
        var applyDataNow = this.applyOnChangedLevel ? levelChanged : true;
        if (applyDataNow) {
            // TODO: currently drawn clusters should be removed! mark them as invisible?
            this.clearCache();
            this.filterData();
            // console.log('clustergenerator: cache cleared...'); // TODO: cleanup
        }
    }

    this.cacheLevel = level;
    var clusters = this.cache[level];
    if (!clusters) {
        // console.log('clustergenerator: create cluster level ' + level); // TODO: cleanup
        clusters = [];

        // TODO: spit this method, it is too large
        for (var groupName in this.groups) {
            if (this.groups.hasOwnProperty(groupName)) {
                var items = this.groups[groupName];
                var iMax = items.length;
                var i = 0;
                while (i < iMax) {
                    // find all items around current item, within the timeWindow
                    var item = items[i];
                    var neighbors = 1;  // start at 1, to include itself)

                    // loop through items left from the current item
                    var j = i - 1;
                    while (j >= 0 && (item.center - items[j].center) < timeWindow / 2) {
                        if (!items[j].cluster) {
                            neighbors++;
                        }
                        j--;
                    }

                    // loop through items right from the current item
                    var k = i + 1;
                    while (k < items.length && (items[k].center - item.center) < timeWindow / 2) {
                        neighbors++;
                        k++;
                    }

                    // loop through the created clusters
                    var l = clusters.length - 1;
                    while (l >= 0 && (item.center - clusters[l].center) < timeWindow / 2) {
                        if (item.group == clusters[l].group) {
                            neighbors++;
                        }
                        l--;
                    }

                    // aggregate until the number of items is within maxItems
                    if (neighbors > maxItems) {
                        // too busy in this window.
                        var num = neighbors - maxItems + 1;
                        var clusterItems = [];

                        // append the items to the cluster,
                        // and calculate the average start for the cluster
                        var avg = undefined;  // number. average of all start dates
                        var min = undefined;  // number. minimum of all start dates
                        var max = undefined;  // number. maximum of all start and end dates
                        var containsRanges = false;
                        var count = 0;
                        var m = i;
                        while (clusterItems.length < num && m < items.length) {
                            var p = items[m];
                            var start = p.start.valueOf();
                            var end = p.end ? p.end.valueOf() : p.start.valueOf();
                            clusterItems.push(p);
                            if (count) {
                                // calculate new average (use fractions to prevent overflow)
                                avg = (count / (count + 1)) * avg + (1 / (count + 1)) * p.center;
                            }
                            else {
                                avg = p.center;
                            }
                            min = (min != undefined) ? Math.min(min, start) : start;
                            max = (max != undefined) ? Math.max(max, end) : end;
                            containsRanges = containsRanges || (p instanceof links.Timeline.ItemRange);
                            count++;
                            m++;
                        }

                        var cluster;
                        var title = 'Cluster containing ' + count +
                            ' events. Zoom in to see the individual events.';
                        var content = '<div title="' + title + '">' + count + ' events</div>';
                        var group = item.group ? item.group.content : undefined;
                        if (containsRanges) {
                            // boxes and/or ranges
                            cluster = this.timeline.createItem({
                                'start': new Date(min),
                                'end': new Date(max),
                                'content': content,
                                'group': group
                            });
                        }
                        else {
                            // boxes only
                            cluster = this.timeline.createItem({
                                'start': new Date(avg),
                                'content': content,
                                'group': group
                            });
                        }
                        cluster.isCluster = true;
                        cluster.items = clusterItems;
                        cluster.items.forEach(function (item) {
                            item.cluster = cluster;
                        });

                        clusters.push(cluster);
                        i += num;
                    }
                    else {
                        delete item.cluster;
                        i += 1;
                    }
                }
            }
        }

        this.cache[level] = clusters;
    }

    return clusters;
};


/** ------------------------------------------------------------------------ **/


/**
 * Event listener (singleton)
 */
links.events = links.events || {
    'listeners': [],

    /**
     * Find a single listener by its object
     * @param {Object} object
     * @return {Number} index  -1 when not found
     */
    'indexOf': function (object) {
        var listeners = this.listeners;
        for (var i = 0, iMax = this.listeners.length; i < iMax; i++) {
            var listener = listeners[i];
            if (listener && listener.object == object) {
                return i;
            }
        }
        return -1;
    },

    /**
     * Add an event listener
     * @param {Object} object
     * @param {String} event       The name of an event, for example 'select'
     * @param {function} callback  The callback method, called when the
     *                             event takes place
     */
    'addListener': function (object, event, callback) {
        var index = this.indexOf(object);
        var listener = this.listeners[index];
        if (!listener) {
            listener = {
                'object': object,
                'events': {}
            };
            this.listeners.push(listener);
        }

        var callbacks = listener.events[event];
        if (!callbacks) {
            callbacks = [];
            listener.events[event] = callbacks;
        }

        // add the callback if it does not yet exist
        if (callbacks.indexOf(callback) == -1) {
            callbacks.push(callback);
        }
    },

    /**
     * Remove an event listener
     * @param {Object} object
     * @param {String} event       The name of an event, for example 'select'
     * @param {function} callback  The registered callback method
     */
    'removeListener': function (object, event, callback) {
        var index = this.indexOf(object);
        var listener = this.listeners[index];
        if (listener) {
            var callbacks = listener.events[event];
            if (callbacks) {
                var index = callbacks.indexOf(callback);
                if (index != -1) {
                    callbacks.splice(index, 1);
                }

                // remove the array when empty
                if (callbacks.length == 0) {
                    delete listener.events[event];
                }
            }

            // count the number of registered events. remove listener when empty
            var count = 0;
            var events = listener.events;
            for (var e in events) {
                if (events.hasOwnProperty(e)) {
                    count++;
                }
            }
            if (count == 0) {
                delete this.listeners[index];
            }
        }
    },

    /**
     * Remove all registered event listeners
     */
    'removeAllListeners': function () {
        this.listeners = [];
    },

    /**
     * Trigger an event. All registered event handlers will be called
     * @param {Object} object
     * @param {String} event
     * @param {Object} properties (optional)
     */
    'trigger': function (object, event, properties) {
        var index = this.indexOf(object);
        var listener = this.listeners[index];
        if (listener) {
            var callbacks = listener.events[event];
            if (callbacks) {
                for (var i = 0, iMax = callbacks.length; i < iMax; i++) {
                    callbacks[i](properties);
                }
            }
        }
    }
};


/** ------------------------------------------------------------------------ **/

/**
 * @constructor  links.Timeline.StepDate
 * The class StepDate is an iterator for dates. You provide a start date and an
 * end date. The class itself determines the best scale (step size) based on the
 * provided start Date, end Date, and minimumStep.
 *
 * If minimumStep is provided, the step size is chosen as close as possible
 * to the minimumStep but larger than minimumStep. If minimumStep is not
 * provided, the scale is set to 1 DAY.
 * The minimumStep should correspond with the onscreen size of about 6 characters
 *
 * Alternatively, you can set a scale by hand.
 * After creation, you can initialize the class by executing start(). Then you
 * can iterate from the start date to the end date via next(). You can check if
 * the end date is reached with the function end(). After each step, you can
 * retrieve the current date via get().
 * The class step has scales ranging from milliseconds, seconds, minutes, hours,
 * days, to years.
 *
 * Version: 1.2
 *
 * @param {Date} start          The start date, for example new Date(2010, 9, 21)
 *                              or new Date(2010, 9, 21, 23, 45, 00)
 * @param {Date} end            The end date
 * @param {Number}  minimumStep Optional. Minimum step size in milliseconds
 */
links.Timeline.StepDate = function(start, end, minimumStep) {

    // variables
    this.current = new Date();
    this._start = new Date();
    this._end = new Date();

    this.autoScale  = true;
    this.scale = links.Timeline.StepDate.SCALE.DAY;
    this.step = 1;

    // initialize the range
    this.setRange(start, end, minimumStep);
};

/// enum scale
links.Timeline.StepDate.SCALE = {
    MILLISECOND: 1,
    SECOND: 2,
    MINUTE: 3,
    HOUR: 4,
    DAY: 5,
    WEEKDAY: 6,
    MONTH: 7,
    YEAR: 8
};


/**
 * Set a new range
 * If minimumStep is provided, the step size is chosen as close as possible
 * to the minimumStep but larger than minimumStep. If minimumStep is not
 * provided, the scale is set to 1 DAY.
 * The minimumStep should correspond with the onscreen size of about 6 characters
 * @param {Date} start        The start date and time.
 * @param {Date} end          The end date and time.
 * @param {int}  minimumStep  Optional. Minimum step size in milliseconds
 */
links.Timeline.StepDate.prototype.setRange = function(start, end, minimumStep) {
    if (!(start instanceof Date) || !(end instanceof Date)) {
        //throw  "No legal start or end date in method setRange";
        return;
    }

    this._start = (start != undefined) ? new Date(start.valueOf()) : new Date();
    this._end = (end != undefined) ? new Date(end.valueOf()) : new Date();

    if (this.autoScale) {
        this.setMinimumStep(minimumStep);
    }
};

/**
 * Set the step iterator to the start date.
 */
links.Timeline.StepDate.prototype.start = function() {
    this.current = new Date(this._start.valueOf());
    this.roundToMinor();
};

/**
 * Round the current date to the first minor date value
 * This must be executed once when the current date is set to start Date
 */
links.Timeline.StepDate.prototype.roundToMinor = function() {
    // round to floor
    // IMPORTANT: we have no breaks in this switch! (this is no bug)
    //noinspection FallthroughInSwitchStatementJS
    switch (this.scale) {
        case links.Timeline.StepDate.SCALE.YEAR:
            this.current.setFullYear(this.step * Math.floor(this.current.getFullYear() / this.step));
            this.current.setMonth(0);
        case links.Timeline.StepDate.SCALE.MONTH:        this.current.setDate(1);
        case links.Timeline.StepDate.SCALE.DAY:          // intentional fall through
        case links.Timeline.StepDate.SCALE.WEEKDAY:      this.current.setHours(0);
        case links.Timeline.StepDate.SCALE.HOUR:         this.current.setMinutes(0);
        case links.Timeline.StepDate.SCALE.MINUTE:       this.current.setSeconds(0);
        case links.Timeline.StepDate.SCALE.SECOND:       this.current.setMilliseconds(0);
        //case links.Timeline.StepDate.SCALE.MILLISECOND: // nothing to do for milliseconds
    }

    if (this.step != 1) {
        // round down to the first minor value that is a multiple of the current step size
        switch (this.scale) {
            case links.Timeline.StepDate.SCALE.MILLISECOND:  this.current.setMilliseconds(this.current.getMilliseconds() - this.current.getMilliseconds() % this.step);  break;
            case links.Timeline.StepDate.SCALE.SECOND:       this.current.setSeconds(this.current.getSeconds() - this.current.getSeconds() % this.step); break;
            case links.Timeline.StepDate.SCALE.MINUTE:       this.current.setMinutes(this.current.getMinutes() - this.current.getMinutes() % this.step); break;
            case links.Timeline.StepDate.SCALE.HOUR:         this.current.setHours(this.current.getHours() - this.current.getHours() % this.step); break;
            case links.Timeline.StepDate.SCALE.WEEKDAY:      // intentional fall through
            case links.Timeline.StepDate.SCALE.DAY:          this.current.setDate((this.current.getDate()-1) - (this.current.getDate()-1) % this.step + 1); break;
            case links.Timeline.StepDate.SCALE.MONTH:        this.current.setMonth(this.current.getMonth() - this.current.getMonth() % this.step);  break;
            case links.Timeline.StepDate.SCALE.YEAR:         this.current.setFullYear(this.current.getFullYear() - this.current.getFullYear() % this.step); break;
            default: break;
        }
    }
};

/**
 * Check if the end date is reached
 * @return {boolean}  true if the current date has passed the end date
 */
links.Timeline.StepDate.prototype.end = function () {
    return (this.current.valueOf() > this._end.valueOf());
};

/**
 * Do the next step
 */
links.Timeline.StepDate.prototype.next = function() {
    var prev = this.current.valueOf();

    // Two cases, needed to prevent issues with switching daylight savings 
    // (end of March and end of October)
    if (this.current.getMonth() < 6)   {
        switch (this.scale) {
            case links.Timeline.StepDate.SCALE.MILLISECOND:

                this.current = new Date(this.current.valueOf() + this.step); break;
            case links.Timeline.StepDate.SCALE.SECOND:       this.current = new Date(this.current.valueOf() + this.step * 1000); break;
            case links.Timeline.StepDate.SCALE.MINUTE:       this.current = new Date(this.current.valueOf() + this.step * 1000 * 60); break;
            case links.Timeline.StepDate.SCALE.HOUR:
                this.current = new Date(this.current.valueOf() + this.step * 1000 * 60 * 60);
                // in case of skipping an hour for daylight savings, adjust the hour again (else you get: 0h 5h 9h ... instead of 0h 4h 8h ...)
                var h = this.current.getHours();
                this.current.setHours(h - (h % this.step));
                break;
            case links.Timeline.StepDate.SCALE.WEEKDAY:      // intentional fall through
            case links.Timeline.StepDate.SCALE.DAY:          this.current.setDate(this.current.getDate() + this.step); break;
            case links.Timeline.StepDate.SCALE.MONTH:        this.current.setMonth(this.current.getMonth() + this.step); break;
            case links.Timeline.StepDate.SCALE.YEAR:         this.current.setFullYear(this.current.getFullYear() + this.step); break;
            default:                      break;
        }
    }
    else {
        switch (this.scale) {
            case links.Timeline.StepDate.SCALE.MILLISECOND:  this.current = new Date(this.current.valueOf() + this.step); break;
            case links.Timeline.StepDate.SCALE.SECOND:       this.current.setSeconds(this.current.getSeconds() + this.step); break;
            case links.Timeline.StepDate.SCALE.MINUTE:       this.current.setMinutes(this.current.getMinutes() + this.step); break;
            case links.Timeline.StepDate.SCALE.HOUR:         this.current.setHours(this.current.getHours() + this.step); break;
            case links.Timeline.StepDate.SCALE.WEEKDAY:      // intentional fall through
            case links.Timeline.StepDate.SCALE.DAY:          this.current.setDate(this.current.getDate() + this.step); break;
            case links.Timeline.StepDate.SCALE.MONTH:        this.current.setMonth(this.current.getMonth() + this.step); break;
            case links.Timeline.StepDate.SCALE.YEAR:         this.current.setFullYear(this.current.getFullYear() + this.step); break;
            default:                      break;
        }
    }

    if (this.step != 1) {
        // round down to the correct major value
        switch (this.scale) {
            case links.Timeline.StepDate.SCALE.MILLISECOND:  if(this.current.getMilliseconds() < this.step) this.current.setMilliseconds(0);  break;
            case links.Timeline.StepDate.SCALE.SECOND:       if(this.current.getSeconds() < this.step) this.current.setSeconds(0);  break;
            case links.Timeline.StepDate.SCALE.MINUTE:       if(this.current.getMinutes() < this.step) this.current.setMinutes(0);  break;
            case links.Timeline.StepDate.SCALE.HOUR:         if(this.current.getHours() < this.step) this.current.setHours(0);  break;
            case links.Timeline.StepDate.SCALE.WEEKDAY:      // intentional fall through
            case links.Timeline.StepDate.SCALE.DAY:          if(this.current.getDate() < this.step+1) this.current.setDate(1); break;
            case links.Timeline.StepDate.SCALE.MONTH:        if(this.current.getMonth() < this.step) this.current.setMonth(0);  break;
            case links.Timeline.StepDate.SCALE.YEAR:         break; // nothing to do for year
            default:                break;
        }
    }

    // safety mechanism: if current time is still unchanged, move to the end
    if (this.current.valueOf() == prev) {
        this.current = new Date(this._end.valueOf());
    }
};


/**
 * Get the current datetime
 * @return {Date}  current The current date
 */
links.Timeline.StepDate.prototype.getCurrent = function() {
    return this.current;
};

/**
 * Set a custom scale. Autoscaling will be disabled.
 * For example setScale(SCALE.MINUTES, 5) will result
 * in minor steps of 5 minutes, and major steps of an hour.
 *
 * @param {links.Timeline.StepDate.SCALE} newScale
 *                               A scale. Choose from SCALE.MILLISECOND,
 *                               SCALE.SECOND, SCALE.MINUTE, SCALE.HOUR,
 *                               SCALE.WEEKDAY, SCALE.DAY, SCALE.MONTH,
 *                               SCALE.YEAR.
 * @param {Number}     newStep   A step size, by default 1. Choose for
 *                               example 1, 2, 5, or 10.
 */
links.Timeline.StepDate.prototype.setScale = function(newScale, newStep) {
    this.scale = newScale;

    if (newStep > 0) {
        this.step = newStep;
    }

    this.autoScale = false;
};

/**
 * Enable or disable autoscaling
 * @param {boolean} enable  If true, autoascaling is set true
 */
links.Timeline.StepDate.prototype.setAutoScale = function (enable) {
    this.autoScale = enable;
};


/**
 * Automatically determine the scale that bests fits the provided minimum step
 * @param {Number} minimumStep  The minimum step size in milliseconds
 */
links.Timeline.StepDate.prototype.setMinimumStep = function(minimumStep) {
    if (minimumStep == undefined) {
        return;
    }

    var stepYear       = (1000 * 60 * 60 * 24 * 30 * 12);
    var stepMonth      = (1000 * 60 * 60 * 24 * 30);
    var stepDay        = (1000 * 60 * 60 * 24);
    var stepHour       = (1000 * 60 * 60);
    var stepMinute     = (1000 * 60);
    var stepSecond     = (1000);
    var stepMillisecond= (1);

    // find the smallest step that is larger than the provided minimumStep
    if (stepYear*1000 > minimumStep)        {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 1000;}
    if (stepYear*500 > minimumStep)         {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 500;}
    if (stepYear*100 > minimumStep)         {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 100;}
    if (stepYear*50 > minimumStep)          {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 50;}
    if (stepYear*10 > minimumStep)          {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 10;}
    if (stepYear*5 > minimumStep)           {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 5;}
    if (stepYear > minimumStep)             {this.scale = links.Timeline.StepDate.SCALE.YEAR;        this.step = 1;}
    if (stepMonth*3 > minimumStep)          {this.scale = links.Timeline.StepDate.SCALE.MONTH;       this.step = 3;}
    if (stepMonth > minimumStep)            {this.scale = links.Timeline.StepDate.SCALE.MONTH;       this.step = 1;}
    if (stepDay*5 > minimumStep)            {this.scale = links.Timeline.StepDate.SCALE.DAY;         this.step = 5;}
    if (stepDay*2 > minimumStep)            {this.scale = links.Timeline.StepDate.SCALE.DAY;         this.step = 2;}
    if (stepDay > minimumStep)              {this.scale = links.Timeline.StepDate.SCALE.DAY;         this.step = 1;}
    if (stepDay/2 > minimumStep)            {this.scale = links.Timeline.StepDate.SCALE.WEEKDAY;     this.step = 1;}
    if (stepHour*4 > minimumStep)           {this.scale = links.Timeline.StepDate.SCALE.HOUR;        this.step = 4;}
    if (stepHour > minimumStep)             {this.scale = links.Timeline.StepDate.SCALE.HOUR;        this.step = 1;}
    if (stepMinute*15 > minimumStep)        {this.scale = links.Timeline.StepDate.SCALE.MINUTE;      this.step = 15;}
    if (stepMinute*10 > minimumStep)        {this.scale = links.Timeline.StepDate.SCALE.MINUTE;      this.step = 10;}
    if (stepMinute*5 > minimumStep)         {this.scale = links.Timeline.StepDate.SCALE.MINUTE;      this.step = 5;}
    if (stepMinute > minimumStep)           {this.scale = links.Timeline.StepDate.SCALE.MINUTE;      this.step = 1;}
    if (stepSecond*15 > minimumStep)        {this.scale = links.Timeline.StepDate.SCALE.SECOND;      this.step = 15;}
    if (stepSecond*10 > minimumStep)        {this.scale = links.Timeline.StepDate.SCALE.SECOND;      this.step = 10;}
    if (stepSecond*5 > minimumStep)         {this.scale = links.Timeline.StepDate.SCALE.SECOND;      this.step = 5;}
    if (stepSecond > minimumStep)           {this.scale = links.Timeline.StepDate.SCALE.SECOND;      this.step = 1;}
    if (stepMillisecond*200 > minimumStep)  {this.scale = links.Timeline.StepDate.SCALE.MILLISECOND; this.step = 200;}
    if (stepMillisecond*100 > minimumStep)  {this.scale = links.Timeline.StepDate.SCALE.MILLISECOND; this.step = 100;}
    if (stepMillisecond*50 > minimumStep)   {this.scale = links.Timeline.StepDate.SCALE.MILLISECOND; this.step = 50;}
    if (stepMillisecond*10 > minimumStep)   {this.scale = links.Timeline.StepDate.SCALE.MILLISECOND; this.step = 10;}
    if (stepMillisecond*5 > minimumStep)    {this.scale = links.Timeline.StepDate.SCALE.MILLISECOND; this.step = 5;}
    if (stepMillisecond > minimumStep)      {this.scale = links.Timeline.StepDate.SCALE.MILLISECOND; this.step = 1;}
};

/**
 * Snap a date to a rounded value. The snap intervals are dependent on the
 * current scale and step.
 * @param {Date} date   the date to be snapped
 */
links.Timeline.StepDate.prototype.snap = function(date) {
    if (this.scale == links.Timeline.StepDate.SCALE.YEAR) {
        var year = date.getFullYear() + Math.round(date.getMonth() / 12);
        date.setFullYear(Math.round(year / this.step) * this.step);
        date.setMonth(0);
        date.setDate(0);
        date.setHours(0);
        date.setMinutes(0);
        date.setSeconds(0);
        date.setMilliseconds(0);
    }
    else if (this.scale == links.Timeline.StepDate.SCALE.MONTH) {
        if (date.getDate() > 15) {
            date.setDate(1);
            date.setMonth(date.getMonth() + 1);
            // important: first set Date to 1, after that change the month.      
        }
        else {
            date.setDate(1);
        }

        date.setHours(0);
        date.setMinutes(0);
        date.setSeconds(0);
        date.setMilliseconds(0);
    }
    else if (this.scale == links.Timeline.StepDate.SCALE.DAY ||
        this.scale == links.Timeline.StepDate.SCALE.WEEKDAY) {
        switch (this.step) {
            case 5:
            case 2:
                date.setHours(Math.round(date.getHours() / 24) * 24); break;
            default:
                date.setHours(Math.round(date.getHours() / 12) * 12); break;
        }
        date.setMinutes(0);
        date.setSeconds(0);
        date.setMilliseconds(0);
    }
    else if (this.scale == links.Timeline.StepDate.SCALE.HOUR) {
        switch (this.step) {
            case 4:
                date.setMinutes(Math.round(date.getMinutes() / 60) * 60); break;
            default:
                date.setMinutes(Math.round(date.getMinutes() / 30) * 30); break;
        }
        date.setSeconds(0);
        date.setMilliseconds(0);
    } else if (this.scale == links.Timeline.StepDate.SCALE.MINUTE) {
        switch (this.step) {
            case 15:
            case 10:
                date.setMinutes(Math.round(date.getMinutes() / 5) * 5);
                date.setSeconds(0);
                break;
            case 5:
                date.setSeconds(Math.round(date.getSeconds() / 60) * 60); break;
            default:
                date.setSeconds(Math.round(date.getSeconds() / 30) * 30); break;
        }
        date.setMilliseconds(0);
    }
    else if (this.scale == links.Timeline.StepDate.SCALE.SECOND) {
        switch (this.step) {
            case 15:
            case 10:
                date.setSeconds(Math.round(date.getSeconds() / 5) * 5);
                date.setMilliseconds(0);
                break;
            case 5:
                date.setMilliseconds(Math.round(date.getMilliseconds() / 1000) * 1000); break;
            default:
                date.setMilliseconds(Math.round(date.getMilliseconds() / 500) * 500); break;
        }
    }
    else if (this.scale == links.Timeline.StepDate.SCALE.MILLISECOND) {
        var step = this.step > 5 ? this.step / 2 : 1;
        date.setMilliseconds(Math.round(date.getMilliseconds() / step) * step);
    }
};

/**
 * Check if the current step is a major step (for example when the step
 * is DAY, a major step is each first day of the MONTH)
 * @return {boolean} true if current date is major, else false.
 */
links.Timeline.StepDate.prototype.isMajor = function() {
    switch (this.scale) {
        case links.Timeline.StepDate.SCALE.MILLISECOND:
            return (this.current.getMilliseconds() == 0);
        case links.Timeline.StepDate.SCALE.SECOND:
            return (this.current.getSeconds() == 0);
        case links.Timeline.StepDate.SCALE.MINUTE:
            return (this.current.getHours() == 0) && (this.current.getMinutes() == 0);
        // Note: this is no bug. Major label is equal for both minute and hour scale
        case links.Timeline.StepDate.SCALE.HOUR:
            return (this.current.getHours() == 0);
        case links.Timeline.StepDate.SCALE.WEEKDAY: // intentional fall through
        case links.Timeline.StepDate.SCALE.DAY:
            return (this.current.getDate() == 1);
        case links.Timeline.StepDate.SCALE.MONTH:
            return (this.current.getMonth() == 0);
        case links.Timeline.StepDate.SCALE.YEAR:
            return false;
        default:
            return false;
    }
};


/**
 * Returns formatted text for the minor axislabel, depending on the current
 * date and the scale. For example when scale is MINUTE, the current time is
 * formatted as "hh:mm".
 * @param {Object} options
 * @param {Date} [date] custom date. if not provided, current date is taken
 */
links.Timeline.StepDate.prototype.getLabelMinor = function(options, date) {
    if (date == undefined) {
        date = this.current;
    }

    switch (this.scale) {
        case links.Timeline.StepDate.SCALE.MILLISECOND:  return String(date.getMilliseconds());
        case links.Timeline.StepDate.SCALE.SECOND:       return String(date.getSeconds());
        case links.Timeline.StepDate.SCALE.MINUTE:
            return this.addZeros(date.getHours(), 2) + ":" + this.addZeros(date.getMinutes(), 2);
        case links.Timeline.StepDate.SCALE.HOUR:
            return this.addZeros(date.getHours(), 2) + ":" + this.addZeros(date.getMinutes(), 2);
        case links.Timeline.StepDate.SCALE.WEEKDAY:      return options.DAYS_SHORT[date.getDay()] + ' ' + date.getDate();
        case links.Timeline.StepDate.SCALE.DAY:          return String(date.getDate());
        case links.Timeline.StepDate.SCALE.MONTH:        return options.MONTHS_SHORT[date.getMonth()];   // month is zero based
        case links.Timeline.StepDate.SCALE.YEAR:         return String(date.getFullYear());
        default:                                         return "";
    }
};


/**
 * Returns formatted text for the major axislabel, depending on the current
 * date and the scale. For example when scale is MINUTE, the major scale is
 * hours, and the hour will be formatted as "hh".
 * @param {Object} options
 * @param {Date} [date] custom date. if not provided, current date is taken
 */
links.Timeline.StepDate.prototype.getLabelMajor = function(options, date) {
    if (date == undefined) {
        date = this.current;
    }

    switch (this.scale) {
        case links.Timeline.StepDate.SCALE.MILLISECOND:
            return  this.addZeros(date.getHours(), 2) + ":" +
                this.addZeros(date.getMinutes(), 2) + ":" +
                this.addZeros(date.getSeconds(), 2);
        case links.Timeline.StepDate.SCALE.SECOND:
            return  date.getDate() + " " +
                options.MONTHS[date.getMonth()] + " " +
                this.addZeros(date.getHours(), 2) + ":" +
                this.addZeros(date.getMinutes(), 2);
        case links.Timeline.StepDate.SCALE.MINUTE:
            return  options.DAYS[date.getDay()] + " " +
                date.getDate() + " " +
                options.MONTHS[date.getMonth()] + " " +
                date.getFullYear();
        case links.Timeline.StepDate.SCALE.HOUR:
            return  options.DAYS[date.getDay()] + " " +
                date.getDate() + " " +
                options.MONTHS[date.getMonth()] + " " +
                date.getFullYear();
        case links.Timeline.StepDate.SCALE.WEEKDAY:
        case links.Timeline.StepDate.SCALE.DAY:
            return  options.MONTHS[date.getMonth()] + " " +
                date.getFullYear();
        case links.Timeline.StepDate.SCALE.MONTH:
            return String(date.getFullYear());
        default:
            return "";
    }
};

/**
 * Add leading zeros to the given value to match the desired length.
 * For example addZeros(123, 5) returns "00123"
 * @param {int} value   A value
 * @param {int} len     Desired final length
 * @return {string}     value with leading zeros
 */
links.Timeline.StepDate.prototype.addZeros = function(value, len) {
    var str = "" + value;
    while (str.length < len) {
        str = "0" + str;
    }
    return str;
};



/** ------------------------------------------------------------------------ **/

/**
 * Image Loader service.
 * can be used to get a callback when a certain image is loaded
 *
 */
links.imageloader = (function () {
    var urls = {};  // the loaded urls
    var callbacks = {}; // the urls currently being loaded. Each key contains 
    // an array with callbacks

    /**
     * Check if an image url is loaded
     * @param {String} url
     * @return {boolean} loaded   True when loaded, false when not loaded
     *                            or when being loaded
     */
    function isLoaded (url) {
        if (urls[url] == true) {
            return true;
        }

        var image = new Image();
        image.src = url;
        if (image.complete) {
            return true;
        }

        return false;
    }

    /**
     * Check if an image url is being loaded
     * @param {String} url
     * @return {boolean} loading   True when being loaded, false when not loading
     *                             or when already loaded
     */
    function isLoading (url) {
        return (callbacks[url] != undefined);
    }

    /**
     * Load given image url
     * @param {String} url
     * @param {function} callback
     * @param {boolean} sendCallbackWhenAlreadyLoaded  optional
     */
    function load (url, callback, sendCallbackWhenAlreadyLoaded) {
        if (sendCallbackWhenAlreadyLoaded == undefined) {
            sendCallbackWhenAlreadyLoaded = true;
        }

        if (isLoaded(url)) {
            if (sendCallbackWhenAlreadyLoaded) {
                callback(url);
            }
            return;
        }

        if (isLoading(url) && !sendCallbackWhenAlreadyLoaded) {
            return;
        }

        var c = callbacks[url];
        if (!c) {
            var image = new Image();
            image.src = url;

            c = [];
            callbacks[url] = c;

            image.onload = function (event) {
                urls[url] = true;
                delete callbacks[url];

                for (var i = 0; i < c.length; i++) {
                    c[i](url);
                }
            }
        }

        if (c.indexOf(callback) == -1) {
            c.push(callback);
        }
    }

    /**
     * Load a set of images, and send a callback as soon as all images are
     * loaded
     * @param {String[]} urls
     * @param {function } callback
     * @param {boolean} sendCallbackWhenAlreadyLoaded
     */
    function loadAll (urls, callback, sendCallbackWhenAlreadyLoaded) {
        // list all urls which are not yet loaded
        var urlsLeft = [];
        urls.forEach(function (url) {
            if (!isLoaded(url)) {
                urlsLeft.push(url);
            }
        });

        if (urlsLeft.length) {
            // there are unloaded images
            var countLeft = urlsLeft.length;
            urlsLeft.forEach(function (url) {
                load(url, function () {
                    countLeft--;
                    if (countLeft == 0) {
                        // done!
                        callback();
                    }
                }, sendCallbackWhenAlreadyLoaded);
            });
        }
        else {
            // we are already done!
            if (sendCallbackWhenAlreadyLoaded) {
                callback();
            }
        }
    }

    /**
     * Recursively retrieve all image urls from the images located inside a given
     * HTML element
     * @param {Node} elem
     * @param {String[]} urls   Urls will be added here (no duplicates)
     */
    function filterImageUrls (elem, urls) {
        var child = elem.firstChild;
        while (child) {
            if (child.tagName == 'IMG') {
                var url = child.src;
                if (urls.indexOf(url) == -1) {
                    urls.push(url);
                }
            }

            filterImageUrls(child, urls);

            child = child.nextSibling;
        }
    }

    return {
        'isLoaded': isLoaded,
        'isLoading': isLoading,
        'load': load,
        'loadAll': loadAll,
        'filterImageUrls': filterImageUrls
    };
})();


/** ------------------------------------------------------------------------ **/


/**
 * Add and event listener. Works for all browsers
 * @param {Element} element    An html element
 * @param {string}      action     The action, for example "click",
 *                                 without the prefix "on"
 * @param {function}    listener   The callback function to be executed
 * @param {boolean}     useCapture
 */
links.Timeline.addEventListener = function (element, action, listener, useCapture) {
    if (element.addEventListener) {
        if (useCapture === undefined)
            useCapture = false;

        if (action === "mousewheel" && navigator.userAgent.indexOf("Firefox") >= 0) {
            action = "DOMMouseScroll";  // For Firefox
        }

        element.addEventListener(action, listener, useCapture);
    } else {
        element.attachEvent("on" + action, listener);  // IE browsers
    }
};

/**
 * Remove an event listener from an element
 * @param {Element}  element   An html dom element
 * @param {string}       action    The name of the event, for example "mousedown"
 * @param {function}     listener  The listener function
 * @param {boolean}      useCapture
 */
links.Timeline.removeEventListener = function(element, action, listener, useCapture) {
    if (element.removeEventListener) {
        // non-IE browsers
        if (useCapture === undefined)
            useCapture = false;

        if (action === "mousewheel" && navigator.userAgent.indexOf("Firefox") >= 0) {
            action = "DOMMouseScroll";  // For Firefox
        }

        element.removeEventListener(action, listener, useCapture);
    } else {
        // IE browsers
        element.detachEvent("on" + action, listener);
    }
};


/**
 * Get HTML element which is the target of the event
 * @param {Event} event
 * @return {Element} target element
 */
links.Timeline.getTarget = function (event) {
    // code from http://www.quirksmode.org/js/events_properties.html
    if (!event) {
        event = window.event;
    }

    var target;

    if (event.target) {
        target = event.target;
    }
    else if (event.srcElement) {
        target = event.srcElement;
    }

    if (target.nodeType != undefined && target.nodeType == 3) {
        // defeat Safari bug
        target = target.parentNode;
    }

    return target;
};

/**
 * Stop event propagation
 */
links.Timeline.stopPropagation = function (event) {
    if (!event)
        event = window.event;

    if (event.stopPropagation) {
        event.stopPropagation();  // non-IE browsers
    }
    else {
        event.cancelBubble = true;  // IE browsers
    }
};


/**
 * Cancels the event if it is cancelable, without stopping further propagation of the event.
 */
links.Timeline.preventDefault = function (event) {
    if (!event)
        event = window.event;

    if (event.preventDefault) {
        event.preventDefault();  // non-IE browsers
    }
    else {
        event.returnValue = false;  // IE browsers
    }
};


/**
 * Retrieve the absolute left value of a DOM element
 * @param {Element} elem        A dom element, for example a div
 * @return {number} left        The absolute left position of this element
 *                              in the browser page.
 */
links.Timeline.getAbsoluteLeft = function(elem) {
    var doc = document.documentElement;
    var body = document.body;

    var left = elem.offsetLeft;
    var e = elem.offsetParent;
    while (e != null && e != body && e != doc) {
        left += e.offsetLeft;
        left -= e.scrollLeft;
        e = e.offsetParent;
    }
    return left;
};

/**
 * Retrieve the absolute top value of a DOM element
 * @param {Element} elem        A dom element, for example a div
 * @return {number} top        The absolute top position of this element
 *                              in the browser page.
 */
links.Timeline.getAbsoluteTop = function(elem) {
    var doc = document.documentElement;
    var body = document.body;

    var top = elem.offsetTop;
    var e = elem.offsetParent;
    while (e != null && e != body && e != doc) {
        top += e.offsetTop;
        top -= e.scrollTop;
        e = e.offsetParent;
    }
    return top;
};

/**
 * Get the absolute, vertical mouse position from an event.
 * @param {Event} event
 * @return {Number} pageY
 */
links.Timeline.getPageY = function (event) {
    if (('targetTouches' in event) && event.targetTouches.length) {
        event = event.targetTouches[0];
    }

    if ('pageY' in event) {
        return event.pageY;
    }

    // calculate pageY from clientY
    var clientY = event.clientY;
    var doc = document.documentElement;
    var body = document.body;
    return clientY +
        ( doc && doc.scrollTop || body && body.scrollTop || 0 ) -
        ( doc && doc.clientTop || body && body.clientTop || 0 );
};

/**
 * Get the absolute, horizontal mouse position from an event.
 * @param {Event} event
 * @return {Number} pageX
 */
links.Timeline.getPageX = function (event) {
    if (('targetTouches' in event) && event.targetTouches.length) {
        event = event.targetTouches[0];
    }

    if ('pageX' in event) {
        return event.pageX;
    }

    // calculate pageX from clientX
    var clientX = event.clientX;
    var doc = document.documentElement;
    var body = document.body;
    return clientX +
        ( doc && doc.scrollLeft || body && body.scrollLeft || 0 ) -
        ( doc && doc.clientLeft || body && body.clientLeft || 0 );
};

/**
 * Adds one or more className's to the given elements style
 * @param {Element} elem
 * @param {String} className
 */
links.Timeline.addClassName = function(elem, className) {
    var classes = elem.className.split(' ');
    var classesToAdd = className.split(' ');
    
    var added = false;
    for (var i=0; i<classesToAdd.length; i++) {
        if (classes.indexOf(classesToAdd[i]) == -1) {
            classes.push(classesToAdd[i]); // add the class to the array
            added = true;
        }
    }
    
    if (added) {
        elem.className = classes.join(' ');
    }
};

/**
 * Removes one or more className's from the given elements style
 * @param {Element} elem
 * @param {String} className
 */
links.Timeline.removeClassName = function(elem, className) {
    var classes = elem.className.split(' ');
    var classesToRemove = className.split(' ');
    
    var removed = false;
    for (var i=0; i<classesToRemove.length; i++) {
        var index = classes.indexOf(classesToRemove[i]);
        if (index != -1) {
            classes.splice(index, 1); // remove the class from the array
            removed = true;
        }
    }
    
    if (removed) {
        elem.className = classes.join(' ');
    }
};

/**
 * Check if given object is a Javascript Array
 * @param {*} obj
 * @return {Boolean} isArray    true if the given object is an array
 */
// See http://stackoverflow.com/questions/2943805/javascript-instanceof-typeof-in-gwt-jsni
links.Timeline.isArray = function (obj) {
    if (obj instanceof Array) {
        return true;
    }
    return (Object.prototype.toString.call(obj) === '[object Array]');
};

/**
 * parse a JSON date
 * @param {Date | String | Number} date    Date object to be parsed. Can be:
 *                                         - a Date object like new Date(),
 *                                         - a long like 1356970529389,
 *                                         an ISO String like "2012-12-31T16:16:07.213Z",
 *                                         or a .Net Date string like
 *                                         "\/Date(1356970529389)\/"
 * @return {Date} parsedDate
 */
links.Timeline.parseJSONDate = function (date) {
    if (date == undefined) {
        return undefined;
    }

    //test for date
    if (date instanceof Date) {
        return date;
    }

    // test for MS format.
    // FIXME: will fail on a Number
    var m = date.match(/\/Date\((-?\d+)([-\+]?\d{2})?(\d{2})?\)\//i);
    if (m) {
        var offset = m[2]
            ? (3600000 * m[2]) // hrs offset
            + (60000 * m[3] * (m[2] / Math.abs(m[2]))) // mins offset
            : 0;

        return new Date(
            (1 * m[1]) // ticks
                + offset
        );
    }

    // failing that, try to parse whatever we've got.
    return Date.parse(date);
};
