/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

$(function () {

    function startsWith(s, prefix) {
        return s && s.indexOf(prefix) === 0;
    }

    // Constructs map stores modules
    function buildModuleLinkMap() {
        var moduleLinkMap = {};
        $('div.sphinxsidebar a.reference.internal').each(function (_, a) {
            var href = $(a).attr('href');
            // Whether is a module.
            if (startsWith(href, '#module-')) {
                var moduleName = href.substr(8);
                moduleLinkMap[moduleName] = [$(a), null];
            }
        });
        return moduleLinkMap;
    }

    function getDivNoteMap(dd) {
        //Stores the divs that includes 'Special note' such as 'Deprecated',Experimental.
        var divNoteMap = {};
        // <div class="admonition note">
        //      <p class="first admonition-title">Note</p>
        //      <p class="last">WARN: Flink Internal Use Only</p>
        // </div>
        // The structure will get 'WARN: Flink Internal Use Only'
        dd.find('> div.admonition.note > p.last').each(function (_, p) {
            var text = $(p).text();
            // Whether is Deprecated.
            if (!divNoteMap.deprecated && startsWith(text, 'Deprecated')) {
                divNoteMap.deprecated = $(p).parent();
            }
            // Whether is Experimental.
            if (!divNoteMap.experimental && startsWith(text, 'Experimental')) {
                divNoteMap.experimental = $(p).parent();
            }
        });
        return divNoteMap;
    }

    function getPackageName(name) {
        var last_idx = name.lastIndexOf('.');
        return last_idx === -1 ? '' : name.substr(0, last_idx);
    }

    function buildTag(text, cls, tooltip) {
        return '<span class="pys-tag ' + cls + ' hasTooltip">' + text + '<span class="tooltip">'
            + tooltip + '</span></span>'
    }


    var moduleLinkMap = buildModuleLinkMap();

    // The class field of dl label includes 'class','method','classmethod'.
    $('dl.class, dl.function').each(function (_, dl) {

        dl = $(dl);
        var dt = dl.children('dt').eq(0);
        var dd = dl.children('dd').eq(0);

        // Name of the class(method,classmethod).
        // e.g. <dt id="pyflink.table.TableEnvironment"> will get 'pyflink.table.TableEnvironment'
        var name = dt.attr('id');
        // Desc of the class(method,classmethod).
        // e.g. <code class="descname">TableEnvironment</code> will get 'TableEnvironment'
        var descName = dt.find('> .descname').text();
        var divNoteMap = getDivNoteMap(dd);

        if (name) {
            // e.g. 'pyflink.table.TableEnvironment' will get 'pyflink.table'
            var packageName = getPackageName(name);

            var moduleLink = moduleLinkMap[packageName];
            if (moduleLink) {
                if (moduleLink[1] === null) {
                    moduleLink[1] = $('<ul/>');
                    moduleLink[0].parent().append(moduleLink[1]);
                }
                var tags = '';
                if (divNoteMap.experimental) {
                    tags += buildTag('E', 'pys-tag-experimental', 'Experimental');
                    divNoteMap.experimental.addClass('pys-note pys-note-experimental');
                }
                if (divNoteMap.deprecated) {
                    tags += buildTag('D', 'pys-tag-deprecated', 'Deprecated');
                    divNoteMap.deprecated.addClass('pys-note pys-note-deprecated');
                }
                var li = $('<li/>');
                var a = $('<a href="#' + name + '">' + descName + '</a>');
                li.append(a);
                li.append(tags);
                moduleLink[1].append(li);
                moduleLinkMap[name] = [a, null];
            }
        }
    });
});
