function formattedTimeFromTimestamp(timestamp)
{
    var date = new Date(timestamp);
    return date.toLocaleString();
}

function convertTime(time) {        
    var millis= time % 1000;
    time = parseInt(time/1000);
    var seconds = time % 60;
    time = parseInt(time/60);
    var minutes = time % 60;
    time = parseInt(time/60);
    var hours = time % 24;
    var out = "";
    if(hours && hours > 0) out += hours + " " + ((hours == 1)?"hr":"hrs") + " ";
    if(minutes && minutes > 0) out += minutes + " " + ((minutes == 1)?"min":"mins") + " ";
    if(seconds && seconds > 0) out += seconds + " " + ((seconds == 1)?"sec":"secs") + " ";
    if(millis&& millis> 0) out += millis+ " " + ((millis== 1)?"msec":"msecs") + " ";
    return out.trim();
}
/*
function randomColors(total)
{
    var i = 360 / (total - 1); // distribute the colors evenly on the hue range
    var r = []; // hold the generated colors
    for (var x=0; x<total; x++)
    {
        r.push(hsvToRgb(i * x, 100, 100)); // you can also alternate the saturation and value for even more contrast between the colors
    }
    return r;
}

    /**
     * HSV to RGB color conversion
     *
     * H runs from 0 to 360 degrees
     * S and V run from 0 to 100
     *
     * Ported from the excellent java algorithm by Eugene Vishnevsky at:
     * http://www.cs.rit.edu/~ncs/color/t_convert.html
     */
/*
    function hsvToRgb(h, s, v) {
    var r, g, b;
    var i;
    var f, p, q, t;
     
    // Make sure our arguments stay in-range
    h = Math.max(0, Math.min(360, h));
    s = Math.max(0, Math.min(100, s));
    v = Math.max(0, Math.min(100, v));
     
    // We accept saturation and value arguments from 0 to 100 because that's
    // how Photoshop represents those values. Internally, however, the
    // saturation and value are calculated from a range of 0 to 1. We make
    // That conversion here.
    s /= 100;
    v /= 100;
     
    if(s == 0) {
    // Achromatic (grey)
    r = g = b = v;
    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
    }
     
    h /= 60; // sector 0 to 5
    i = Math.floor(h);
    f = h - i; // factorial part of h
    p = v * (1 - s);
    q = v * (1 - s * f);
    t = v * (1 - s * (1 - f));
     
    switch(i) {
    case 0:
    r = v;
    g = t;
    b = p;
    break;
     
    case 1:
    r = q;
    g = v;
    b = p;
    break;
     
    case 2:
    r = p;
    g = v;
    b = t;
    break;
     
    case 3:
    r = p;
    g = q;
    b = v;
    break;
     
    case 4:
    r = t;
    g = p;
    b = v;
    break;
     
    default: // case 5:
    r = v;
    g = p;
    b = q;
    }
     
    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
    }
*/
