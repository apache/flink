/*
 * Animated Progress Bar
 * - Slightly modified version of the {}DailyCodimg.com Animated Progress Bar
 *
 * - NOTE: This code requires the jquery javascript library to run.
 */

var ProgressBar = function(divId, cellCount, cellSize)
{
    var index = -1;
    var timerObj = new Timer();

    this.Init = function()
    {
        var str = "<table style='border: solid 1px #e1e1e1;' cellpadding='0' cellspacing='1'><tr>";
        for(var cnt=0; cnt<cellCount; cnt++)
        {
            str += "<td style='width: 30px; height: 30px;'>" +
                   "<div style='background-color: #2222dd;display: none;width: " + cellSize + "px; height: " + cellSize + "px;' class='progressbarCell" + cnt + "'></div></td>";
        }
        str += "</tr></table>";
        $("#" + divId).append(str);

        timerObj.Interval = 100;
        timerObj.Tick = timer_tick;
    }

    this.Teardown = function()
    {
        $("#" + divId).remove();
    }
    
    this.Start = function()
    {
        timerObj.Start();
    }

    this.Stop = function()
    {
        timerObj.Stop();
    }

    function timer_tick()
    {
        //debugger;
        index = index + 1;
        index = index % cellCount;

        $("#" + divId + " .progressbarCell" + index).fadeIn(10);
        $("#" + divId + " .progressbarCell" + index).fadeOut(500);
    }
}
    
// Declaring class "Timer"
var Timer = function()
{        
    // Property: Frequency of elapse event of the timer in millisecond
    this.Interval = 1000;

    // Property: Whether the timer is enable or not
    this.Enable = new Boolean(false);

    // Event: Timer tick
    this.Tick;
    
    // Member variable: Hold interval id of the timer
    var timerId = 0;

    // Member variable: Hold instance of this class
    var thisObject;
        
    // Function: Start the timer
    this.Start = function()
    {
        this.Enable = new Boolean(true);

        thisObject = this;
        if (thisObject.Enable)
        {
            thisObject.timerId = setInterval(
            function()
            {
                thisObject.Tick(); 
            }, thisObject.Interval);
        }
    };

    // Function: Stops the timer
    this.Stop = function()
    {            
        thisObject.Enable = new Boolean(false);
        clearInterval(thisObject.timerId);
    };
};