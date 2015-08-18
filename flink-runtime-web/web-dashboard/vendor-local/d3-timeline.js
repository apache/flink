// ----------------------------------------------------------------------------
//  D3 timeline
//  
//  (C) 2015, Jia Huang, published under MIT license
//  https://github.com/jiahuang/d3-timeline
// ----------------------------------------------------------------------------

// vim: ts=2 sw=2
(function () {
  d3.timeline = function() {
    var DISPLAY_TYPES = ["circle", "rect"];

    var hover = function () {},
        mouseover = function () {},
        mouseout = function () {},
        click = function () {},
        scroll = function () {},
        labelFunction = function() {},
        orient = "bottom",
        width = null,
        height = null,
        rowSeperatorsColor = null,
        backgroundColor = null,
        tickFormat = { format: d3.time.format("%I %p"),
          tickTime: d3.time.hours,
          tickInterval: 1,
          tickSize: 6 },
        colorCycle = d3.scale.category20(),
        colorPropertyName = null,
        display = "rect",
        beginning = 0,
        ending = 0,
        margin = {left: 30, right:30, top: 30, bottom:30},
        stacked = false,
        rotateTicks = false,
        timeIsRelative = false,
        itemHeight = 20,
        itemMargin = 5,
        showTimeAxis = true,
        timeAxisTick = false,
        timeAxisTickFormat = {stroke: "stroke-dasharray", spacing: "4 10"},
        showBorderLine = false,
        showHourTimeline = false,
        showBorderFormat = {marginTop: 25, marginBottom: 0, width: 1, color: colorCycle},
        prefix = 'timeline'
      ;

    function timeline (gParent) {
      var gParentSize = gParent[0][0].getBoundingClientRect();
      var gParentItem = d3.select(gParent[0][0]);

      var yAxisMapping = {},
        maxStack = 1,
        minTime = 0,
        maxTime = 0;

      setWidth();

      var gClip = gParent.append("svg:clipPath")
        .attr("id", prefix + "-gclip")
        .append("svg:rect")
        .attr("clipPathUnits","objectBoundingBox")
        .attr("x", margin.left)
        .attr("y", margin.top)
        .attr('width', width - margin.left - margin.right)
        .attr("height", 1000);

      var g = gParent.append("g")
        .attr("clip-path", "url(#" + prefix + "-gclip" + ")")

      // check if the user wants relative time
      // if so, substract the first timestamp from each subsequent timestamps
      if(timeIsRelative){
        g.each(function (d, i) {
          d.forEach(function (datum, index) {
            datum.times.forEach(function (time, j) {
              if(index === 0 && j === 0){
                originTime = time.starting_time;               //Store the timestamp that will serve as origin
                time.starting_time = 0;                        //Set the origin
                time.ending_time = time.ending_time - originTime;     //Store the relative time (millis)
              }else{
                time.starting_time = time.starting_time - originTime;
                time.ending_time = time.ending_time - originTime;
              }
            });
          });
        });
      }

      // check how many stacks we're gonna need
      // do this here so that we can draw the axis before the graph
      if (stacked || ending === 0 || beginning === 0) {
        g.each(function (d, i) {
          d.forEach(function (datum, index) {

            // create y mapping for stacked graph
            if (stacked && Object.keys(yAxisMapping).indexOf(index) == -1) {
              yAxisMapping[index] = maxStack;
              maxStack++;
            }

            // figure out beginning and ending times if they are unspecified
            datum.times.forEach(function (time, i) {
              if(beginning === 0)
                if (time.starting_time < minTime || (minTime === 0 && timeIsRelative === false))
                  minTime = time.starting_time;
              if(ending === 0)
                if (time.ending_time > maxTime)
                  maxTime = time.ending_time;
            });
          });
        });

        if (ending === 0) {
          ending = maxTime;
        }
        if (beginning === 0) {
          beginning = minTime;
        }
      }

      // var scaleFactor = (1/(ending - beginning)) * (width - margin.left - margin.right);

      // draw the axis
      var xScale = d3.time.scale()
        .domain([beginning, ending])
        .range([margin.left, width - margin.right]);

      var xAxis = d3.svg.axis()
        .scale(xScale)
        .orient(orient)
        .tickFormat(tickFormat.format)
        .ticks(tickFormat.numTicks || tickFormat.tickTime, tickFormat.tickInterval)
        .tickSize(tickFormat.tickSize);

      if (showHourTimeline) {
        var xAxis2 = d3.svg.axis()
          .scale(xScale)
          .orient(orient)
          .tickFormat(d3.time.format("%X"))
          .ticks(tickFormat.numTicks || tickFormat.tickTime, tickFormat.tickInterval)
          .tickSize(0);
      }

      if (showTimeAxis) {
        var axisOffsetY = margin.top + (itemHeight + itemMargin) * maxStack;

        g.append("g")
          .attr("class", "axis")
          .attr("transform", "translate(" + 0 +","+axisOffsetY+")")
          .call(xAxis);

        if (showHourTimeline) {
          g.append("g")
            .attr("class", "axis-hour")
            .attr("transform", "translate(" + 0 +","+(axisOffsetY + 20)+")")
            .call(xAxis2);         
        }
      }

      if (timeAxisTick) {
        console.log('here');

        g.append("g")
          .attr("class", "axis axis-tick")
          .attr("transform", "translate(" + 0 +","+
            (margin.top + (itemHeight + itemMargin) * maxStack)+")")
          .attr(timeAxisTickFormat.stroke, timeAxisTickFormat.spacing)
          .call(xAxis.tickFormat("").tickSize(-(margin.top + (itemHeight + itemMargin) * (maxStack - 1) + 3),0,0));
      }
      
      // draw the chart
      g.each(function(d, i) {
        d.forEach( function(datum, index){
          var data = datum.times;
          var hasLabel = (typeof(datum.label) != "undefined");
          var getLabel = function(label){
            if(labelFunction == null){
              return label;
            } else {
              return labelFunction(label);
            }
          };

          // issue warning about using id per data set. Ids should be individual to data elements
          if (typeof(datum.id) != "undefined") {
            console.warn("d3Timeline Warning: Ids per dataset is deprecated in favor of a 'class' key. Ids are now per data element.");
          }

          if (backgroundColor) {
            var greenbarYAxis = ((itemHeight + itemMargin) * yAxisMapping[index]);
            g.selectAll("svg").data(data).enter()
              .insert("rect")
              .attr("class", "row-green-bar")
              .attr("x", 0 + margin.left)
              .attr("width", width - margin.right - margin.left)
              .attr("y", greenbarYAxis)
              .attr("height", itemHeight)
              .attr("fill", backgroundColor)
            ;
          }

          var nel = g.selectAll("svg").data(data).enter().append("g")
            .attr("class", "bar-container")
            .attr("width", getBarWidth);

          nel
            .append("svg:clipPath")
            .attr("id", prefix + "-timeline-textclip-" + i + "-" + index)
            .attr("class", "timeline-clip")
            .append("svg:rect")
            .attr("clipPathUnits","objectBoundingBox")
            .attr("x", getXPos)
            .attr("y", getStackPosition)
            .attr("width", getTextWidth)
            .attr("height", itemHeight);

          nel
            .append(function(d, i) {
              return document.createElementNS(d3.ns.prefix.svg, "display" in d? d.display:display);
            })
            .attr("x", getXPos)
            .attr("y", getStackPosition)
            .attr("width", getBarWidth)
            .attr("cy", function(d, i) {
                return getStackPosition(d, i) + itemHeight/2;
            })
            .attr("cx", getXPos)
            .attr("r", itemHeight / 2)
            .attr("height", itemHeight)
            .style("fill", function(d, i){
              var dColorPropName;
              if (d.color) return d.color;
              if( colorPropertyName ){
                dColorPropName = d[colorPropertyName];
                if ( dColorPropName ) {
                  return colorCycle( dColorPropName );
                } else {
                  return colorCycle( datum[colorPropertyName] );
                }
              }
              return colorCycle(index);
            })
            .on("mousemove", function (d, i) {
              hover(d, index, datum);
            })
            .on("mouseover", function (d, i) {
              mouseover(d, i, datum);
            })
            .on("mouseout", function (d, i) {
              mouseout(d, i, datum);
            })
            .on("click", function (d, i) {
              click(d, index, datum);
            })
            .attr("class", function (d, i) {
              return datum.class ? "timeline-series timelineSeries_"+datum.class : "timeline-series timelineSeries_"+index;
            })
            .attr("id", function(d, i) {
              // use deprecated id field
              if (datum.id && !d.id) {
                return 'timelineItem_'+datum.id;
              }
              
              return d.id ? d.id : "timelineItem_"+index+"_"+i;
            })

          nel
            .append("text")
            .attr("class", "timeline-insidelabel")
            .attr("x", getXTextPos)
            .attr("y", getStackTextPosition)
            .attr("width", getTextWidth)
            .attr("height", itemHeight)
            .attr("clip-path", "url(#" + prefix + "-timeline-textclip-" + i + "-" + index + ")")

            // .attr("width", function (d, i) {
            //   // console.log((d.ending_time - d.starting_time) * scaleFactor);
            //   return (d.ending_time - d.starting_time) * scaleFactor;
            // })
            .text(function(d) {
              return d.label;
            })
            .on("click", function (d, i) {
              click(d, index, datum);
            });
          ;

          if (rowSeperatorsColor) {
            var lineYAxis = ( itemHeight + itemMargin / 2 + margin.top + (itemHeight + itemMargin) * yAxisMapping[index]);
            gParent.append("svg:line")
              .attr("class", "row-seperator")
              .attr("x1", 0 + margin.left)
              .attr("x2", width - margin.right)
              .attr("y1", lineYAxis)
              .attr("y2", lineYAxis)
              .attr("stroke-width", 1)
              .attr("stroke", rowSeperatorsColor);
            ;
          }

          if (showBorderLine) {
            g.selectAll("svg").data(data).enter().append("svg:line")
              .attr("class", "line-" + 'start')
              .attr("x1", getBorderStart)
              // .attr("y1", showBorderFormat.marginTop)
              .attr("y1", getStackPosition)
              .attr("x2", getBorderStart)
              .attr("y2", margin.top + (itemHeight + itemMargin) * maxStack)
              .style("stroke", showBorderFormat.color)
              .style("stroke-width", showBorderFormat.width);

            g.selectAll("svg").data(data).enter().append("svg:line")
              .attr("class", "line-" + 'end')
              .attr("x1", getBorderEnd)
              // .attr("y1", showBorderFormat.marginTop)
              .attr("y1", getStackPosition)
              .attr("x2", getBorderEnd)
              .attr("y2", margin.top + (itemHeight + itemMargin) * maxStack)
              .style("stroke", showBorderFormat.color)
              .style("stroke-width", showBorderFormat.width);
          }

          // add the label
          if (hasLabel) {
            gParent.append("text")
              .attr("class", "timeline-label")
              .attr("transform", "translate("+ 0 +","+ (itemHeight * 0.75 + margin.top + (itemHeight + itemMargin) * yAxisMapping[index])+")")
              .text(hasLabel ? getLabel(datum.label) : datum.id)
              .on("click", function (d, i) {
                click(d, index, datum);
              });
          }

          if (typeof(datum.icon) !== "undefined") {
            gParent.append("image")
              .attr("class", "timeline-label")
              .attr("transform", "translate("+ 0 +","+ (margin.top + (itemHeight + itemMargin) * yAxisMapping[index])+")")
              .attr("xlink:href", datum.icon)
              .attr("width", margin.left)
              .attr("height", itemHeight);
          }

          function getStackPosition(d, i) {
            if (stacked) {
              return margin.top + (itemHeight + itemMargin) * yAxisMapping[index];
            }
            return margin.top;
          }
          function getStackTextPosition(d, i) {
            if (stacked) {
              return margin.top + (itemHeight + itemMargin) * yAxisMapping[index] + itemHeight * 0.70;
            }
            return margin.top + itemHeight * 0.70;
          }
        });
      });

      // if (width > gParentSize.width) {
        var move = function() {

          // var x = Math.min(0, Math.max(gParentSize.width - width, d3.event.translate[0]));
          // zoom.translate([x, 0]);
          // g.attr("transform", "translate(" + x + ",0)");

          // console.log('zoom', d3.event.translate, d3.event.scale);
  
          g.selectAll(".timeline-series")
            .attr("x", getXPos)
            .attr("width", getBarWidth);

          g.selectAll(".timeline-insidelabel")
            .attr("x", getXTextPos)
            .attr("width", getTextWidth);

          g.selectAll(".timeline-clip")
            .attr("x", getXPos)
            .attr("width", getTextWidth);

          g.selectAll(".timeline-clip").select('rect')
            .attr("x", getXPos)
            .attr("width", getTextWidth);

          g.selectAll("g.axis")
            .call(xAxis);

          if (showHourTimeline) {
            g.selectAll("g.axis-hour")
              .call(xAxis2);
          }

          if (showBorderLine) {
            g.selectAll("line.line-start")
              .attr("x1", getBorderStart)
              .attr("x2", getBorderStart);

            g.selectAll("line.line-end")
              .attr("x1", getBorderEnd)
              .attr("x2", getBorderEnd);
          }

          // scroll(x*scaleFactor, xScale);
        };

        var zoom = d3.behavior.zoom().x(xScale).on("zoom", move);

        // gParent
        //   .attr("class", "scrollable")
        //   .call(zoom);

        gParent.call(zoom);
      // }

      if (rotateTicks) {
        g.selectAll(".tick text")
          .attr("transform", function(d) {
            return "rotate(" + rotateTicks + ")translate("
              + (this.getBBox().width / 2 + 10) + "," // TODO: change this 10
              + this.getBBox().height / 2 + ")";
          });
      }

      var gSize = g[0][0].getBoundingClientRect();
      setHeight();

      function getBorderStart(d, i) {
        return xScale(d.starting_time);
      }

      function getBorderEnd(d, i) {
        return xScale(d.ending_time);
      }

      function getXPos(d, i) {
        // return margin.left + (d.starting_time - beginning) * scaleFactor;
        return xScale(d.starting_time);
      }

      function getXTextPos(d, i) {
        // return margin.left + (d.starting_time - beginning) * scaleFactor + 5;
        return xScale(d.starting_time) + 5;
      }

      function getBarWidth(d, i) {
        // return (d.ending_time - d.starting_time) * scaleFactor;
        return xScale(d.ending_time) - xScale(d.starting_time);
      }

      function getTextWidth(d, i) {
        var w = xScale(d.ending_time) - xScale(d.starting_time);
        return  w > 5 ? w - 5 : w;
      }

      function setHeight() {
        if (!height && !gParentItem.attr("height")) {
          if (itemHeight) {
            // set height based off of item height
            height = gSize.height + gSize.top - gParentSize.top;
            // set bounding rectangle height
            d3.select(gParent[0][0]).attr("height", height);
          } else {
            throw "height of the timeline is not set";
          }
        } else {
          if (!height) {
            height = gParentItem.attr("height");
          } else {
            gParentItem.attr("height", height);
          }
        }
      }

      function setWidth() {
        if (!width && !gParentSize.width) {
          try { 
            width = gParentItem.attr("width");
            if (!width) {
              throw "width of the timeline is not set. As of Firefox 27, timeline().with(x) needs to be explicitly set in order to render";
            }            
          } catch (err) {
            console.log( err );
          }
        } else if (!(width && gParentSize.width)) {
          try { 
            width = gParentItem.attr("width");
          } catch (err) {
            console.log( err );
          }
        }
        // if both are set, do nothing
      }
    }

    // SETTINGS

    timeline.margin = function (p) {
      if (!arguments.length) return margin;
      margin = p;
      return timeline;
    };

    timeline.orient = function (orientation) {
      if (!arguments.length) return orient;
      orient = orientation;
      return timeline;
    };

    timeline.itemHeight = function (h) {
      if (!arguments.length) return itemHeight;
      itemHeight = h;
      return timeline;
    };

    timeline.itemMargin = function (h) {
      if (!arguments.length) return itemMargin;
      itemMargin = h;
      return timeline;
    };

    timeline.height = function (h) {
      if (!arguments.length) return height;
      height = h;
      return timeline;
    };

    timeline.width = function (w) {
      if (!arguments.length) return width;
      width = w;
      return timeline;
    };

    timeline.display = function (displayType) {
      if (!arguments.length || (DISPLAY_TYPES.indexOf(displayType) == -1)) return display;
      display = displayType;
      return timeline;
    };

    timeline.labelFormat = function(f) {
      if (!arguments.length) return null;
      labelFunction = f;
      return timeline;
    };

    timeline.tickFormat = function (format) {
      if (!arguments.length) return tickFormat;
      tickFormat = format;
      return timeline;
    };

    timeline.prefix = function (p) {
      if (!arguments.length) return prefix;
      prefix = p;
      return timeline;
    };

    timeline.hover = function (hoverFunc) {
      if (!arguments.length) return hover;
      hover = hoverFunc;
      return timeline;
    };

    timeline.mouseover = function (mouseoverFunc) {
      if (!arguments.length) return mouseoverFunc;
      mouseover = mouseoverFunc;
      return timeline;
    };

    timeline.mouseout = function (mouseoverFunc) {
      if (!arguments.length) return mouseoverFunc;
      mouseout = mouseoverFunc;
      return timeline;
    };

    timeline.click = function (clickFunc) {
      if (!arguments.length) return click;
      click = clickFunc;
      return timeline;
    };

    timeline.scroll = function (scrollFunc) {
      if (!arguments.length) return scroll;
      scroll = scrollFunc;
      return timeline;
    };

    timeline.colors = function (colorFormat) {
      if (!arguments.length) return colorCycle;
      colorCycle = colorFormat;
      return timeline;
    };

    timeline.beginning = function (b) {
      if (!arguments.length) return beginning;
      beginning = b;
      return timeline;
    };

    timeline.ending = function (e) {
      if (!arguments.length) return ending;
      ending = e;
      return timeline;
    };

    timeline.rotateTicks = function (degrees) {
      rotateTicks = degrees;
      return timeline;
    };

    timeline.stack = function () {
      stacked = !stacked;
      return timeline;
    };

    timeline.relativeTime = function() {
      timeIsRelative = !timeIsRelative;
      return timeline;
    };

    timeline.showBorderLine = function () {
        showBorderLine = !showBorderLine;
        return timeline;
    };

    timeline.showHourTimeline = function () {
        showHourTimeline = !showHourTimeline;
        return timeline;
    };

    timeline.showBorderFormat = function(borderFormat) {
      if (!arguments.length) return showBorderFormat;
      showBorderFormat = borderFormat;
      return timeline;
    };

    timeline.colorProperty = function(colorProp) {
      if (!arguments.length) return colorPropertyName;
      colorPropertyName = colorProp;
      return timeline;
    };

    timeline.rowSeperators = function (color) {
      if (!arguments.length) return rowSeperatorsColor;
      rowSeperatorsColor = color;
      return timeline;
    };

    timeline.background = function (color) {
      if (!arguments.length) return backgroundColor;
      backgroundColor = color;
      return timeline;
    };

    timeline.showTimeAxis = function () {
      showTimeAxis = !showTimeAxis;
      return timeline;
    };

    timeline.showTimeAxisTick = function () {
      timeAxisTick = !timeAxisTick;
      return timeline;
    };

    timeline.showTimeAxisTickFormat = function(format) {
      if (!arguments.length) return timeAxisTickFormat;
      timeAxisTickFormat = format;
      return timeline;
    }

    return timeline;
  };
})();
