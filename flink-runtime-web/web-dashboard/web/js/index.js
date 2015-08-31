angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: 'http://localhost:8081',
  "refresh-interval": 10000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
  });
}]).config(["$uiViewScrollProvider", function($uiViewScrollProvider) {
  return $uiViewScrollProvider.useAnchorScroll();
}]).config(["$stateProvider", "$urlRouterProvider", function($stateProvider, $urlRouterProvider) {
  $stateProvider.state("overview", {
    url: "/overview",
    views: {
      main: {
        templateUrl: "partials/overview.html",
        controller: 'OverviewController'
      }
    }
  }).state("running-jobs", {
    url: "/running-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/running-jobs.html",
        controller: 'RunningJobsController'
      }
    }
  }).state("completed-jobs", {
    url: "/completed-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/completed-jobs.html",
        controller: 'CompletedJobsController'
      }
    }
  }).state("single-job", {
    url: "/jobs/{jobid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/jobs/job.html",
        controller: 'SingleJobController'
      }
    }
  }).state("single-job.plan", {
    url: "",
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.node", {
    url: "/vertex/{nodeid}",
    abstract: true,
    views: {
      'node-tabs': {
        templateUrl: "partials/jobs/job.plan.node-tabs.html",
        controller: 'JobPlanNodeTabsController'
      }
    }
  }).state("single-job.plan.node.generic", {
    url: "",
    views: {
      'node-details': {
        controller: 'JobPlanNodeListController',
        templateUrl: "partials/jobs/job.plan.node-list.html"
      }
    }
  }).state("single-job.plan.node.properties", {
    url: "/properties",
    views: {
      'node-details': {
        controller: 'JobPlanNodePropertiesController',
        templateUrl: "partials/jobs/job.plan.node.properties.html"
      }
    }
  }).state("single-job.timeline", {
    url: "/timeline",
    views: {
      details: {
        templateUrl: "partials/jobs/job.timeline.html"
      }
    }
  }).state("single-job.timeline.vertex", {
    url: "/{vertexId}",
    views: {
      vertex: {
        templateUrl: "partials/jobs/job.timeline.vertex.html",
        controller: 'JobTimelineVertexController'
      }
    }
  }).state("single-job.statistics", {
    url: "/statistics",
    views: {
      details: {
        templateUrl: "partials/jobs/job.statistics.html"
      }
    }
  }).state("single-job.exceptions", {
    url: "/exceptions",
    views: {
      details: {
        templateUrl: "partials/jobs/job.exceptions.html",
        controller: 'JobExceptionsController'
      }
    }
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
      }
    }
  });
  return $urlRouterProvider.otherwise("/overview");
}]);

angular.module('flinkApp').directive('bsLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'label label-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('indicatorPrimary', ["JobsService", function(JobsService) {
  return {
    replace: true,
    scope: {
      getLabelClass: "&",
      status: '@'
    },
    template: "<i title='{{status}}' ng-class='getLabelClass()' />",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'fa fa-circle indicator indicator-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('tableProperty', function() {
  return {
    replace: true,
    scope: {
      value: '='
    },
    template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"
  };
});

angular.module('flinkApp').filter("amDurationFormatExtended", ["angularMomentConfig", function(angularMomentConfig) {
  var amDurationFormatExtendedFilter;
  amDurationFormatExtendedFilter = function(value, format, durationFormat) {
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    return moment.duration(value, format).format(durationFormat, {
      trim: false
    });
  };
  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters;
  return amDurationFormatExtendedFilter;
}]).filter("humanizeTaskName", function() {
  return function(text) {
    if (text) {
      return text.replace("&gt;", ">").replace("<br/>", "");
    } else {
      return '';
    }
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var jobObservers;
  jobObservers = [];
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/config").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('RunningJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('running');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('CompletedJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "$rootScope", function($scope, $state, $stateParams, JobsService, $rootScope) {
  $scope.jobid = $stateParams.jobid;
  $rootScope.job = null;
  $rootScope.plan = null;
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    $rootScope.job = data;
    return $rootScope.plan = data.plan;
  });
  return $scope.$on('$destroy', function() {
    $rootScope.job = null;
    return $rootScope.plan = null;
  });
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  console.log('JobPlanController');
  return $scope.stateList = JobsService.stateList();
}]).controller('JobPlanNodeController', ["$scope", "$state", "$stateParams", "JobsService", "$anchorScroll", function($scope, $state, $stateParams, JobsService, $anchorScroll) {
  console.log('JobPlanNodeController');
  $scope.nodeid = $stateParams.nodeid;
  $scope.stateList = JobsService.stateList();
  return JobsService.getNode($scope.nodeid).then(function(data) {
    $scope.node = data;
    return $scope.job.currentNode = data;
  });
}]).controller('JobPlanNodeTabsController', ["$scope", "$stateParams", "JobsService", function($scope, $stateParams, JobsService) {
  console.log('JobPlanNodeTabsController');
  $scope.nodeid = $stateParams.nodeid;
  return JobsService.getNode($scope.nodeid).then(function(data) {
    $scope.node = data;
    return $scope.job.currentNode = data;
  });
}]).controller('JobPlanNodeListController', ["$scope", "$stateParams", "JobsService", function($scope, $stateParams, JobsService) {
  console.log('JobPlanNodeListController');
  return $scope.stateList = JobsService.stateList();
}]).controller('JobPlanNodePropertiesController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return console.log('JobPlanNodePropertiesController');
}]).controller('JobPlanNodeAccumulatorsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return console.log('JobPlanNodeAccumulatorsController');
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.getVertex($stateParams.vertexId).then(function(data) {
    return $scope.vertex = data;
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
}]);

angular.module('flinkApp').directive('vertex', ["$state", function($state) {
  return {
    template: "<svg class='timeline secondary' width='0' height='0'></svg>",
    scope: {
      data: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      analyzeTime = function(data) {
        var chart, svg, testData;
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          return testData.push({
            label: subtask.host + " (" + subtask.subtask + ")",
            times: [
              {
                label: "Scheduled",
                color: "#666",
                borderColor: "#555",
                starting_time: subtask.timestamps["SCHEDULED"],
                ending_time: subtask.timestamps["DEPLOYING"],
                type: 'regular'
              }, {
                label: "Deploying",
                color: "#aaa",
                borderColor: "#555",
                starting_time: subtask.timestamps["DEPLOYING"],
                ending_time: subtask.timestamps["RUNNING"],
                type: 'regular'
              }, {
                label: "Running",
                color: "#ddd",
                borderColor: "#555",
                starting_time: subtask.timestamps["RUNNING"],
                ending_time: subtask.timestamps["FINISHED"],
                type: 'regular'
              }
            ]
          });
        });
        chart = d3.timeline().stack().tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("single").labelFormat(function(label) {
          return label;
        }).margin({
          left: 100,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).relativeTime();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      analyzeTime(scope.data);
    }
  };
}]).directive('timeline', ["$state", function($state) {
  return {
    template: "<svg class='timeline' width='0' height='0'></svg>",
    scope: {
      job: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, translateLabel;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      translateLabel = function(label) {
        return label.replace("&gt;", ">");
      };
      analyzeTime = function(data) {
        var chart, svg, testData;
        testData = [];
        testData.push({
          times: [
            {
              label: "Scheduled",
              color: "#cccccc",
              borderColor: "#555",
              starting_time: data.timestamps["CREATED"],
              ending_time: data.timestamps["CREATED"] + 1,
              type: 'scheduled'
            }
          ]
        });
        angular.forEach(data.vertices, function(vertex) {
          if (vertex['start-time'] > -1) {
            return testData.push({
              times: [
                {
                  label: translateLabel(vertex.name),
                  color: "#d9f1f7",
                  borderColor: "#62cdea",
                  starting_time: vertex['start-time'],
                  ending_time: vertex['end-time'],
                  link: vertex.id,
                  type: 'regular'
                }
              ]
            });
          }
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: data.jid,
              vertexId: d.link
            });
          }
        }).tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("main").margin({
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).showBorderLine().showHourTimeline();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      scope.$watch(attrs.job, function(data) {
        if (data) {
          return analyzeTime(data);
        }
      });
    }
  };
}]).directive('jobPlan', ["$timeout", function($timeout) {
  return {
    template: "<svg class='graph' width='500' height='400'><g /></svg> <svg class='tmp' width='1' height='1'><g /></svg> <div class='btn-group zoom-buttons'> <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a> <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a> </div>",
    scope: {
      plan: '='
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, getNodeType, isSpecialIterationNode, jobid, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, searchForNode, shortenString, subgraphs;
      mainZoom = d3.behavior.zoom();
      subgraphs = [];
      jobid = attrs.jobid;
      mainSvgElement = elem.children()[0];
      mainG = elem.children().children()[0];
      mainTmpElement = elem.children()[1];
      d3mainSvg = d3.select(mainSvgElement);
      d3mainSvgG = d3.select(mainG);
      d3tmpSvg = d3.select(mainTmpElement);
      containerW = elem.width();
      angular.element(elem.children()[0]).width(containerW);
      scope.zoomIn = function() {
        var translate, v1, v2;
        if (mainZoom.scale() < 2.99) {
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          mainZoom.scale(mainZoom.scale() + 0.1);
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      scope.zoomOut = function() {
        var translate, v1, v2;
        if (mainZoom.scale() > 0.31) {
          mainZoom.scale(mainZoom.scale() - 0.1);
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      createLabelEdge = function(el) {
        var labelValue;
        labelValue = "";
        if ((el.ship_strategy != null) || (el.local_strategy != null)) {
          labelValue += "<div class='edge-label'>";
          if (el.ship_strategy != null) {
            labelValue += el.ship_strategy;
          }
          if (el.temp_mode !== undefined) {
            labelValue += " (" + el.temp_mode + ")";
          }
          if (el.local_strategy !== undefined) {
            labelValue += ",<br>" + el.local_strategy;
          }
          labelValue += "</div>";
        }
        return labelValue;
      };
      isSpecialIterationNode = function(info) {
        return info === "partialSolution" || info === "nextPartialSolution" || info === "workset" || info === "nextWorkset" || info === "solutionSet" || info === "solutionDelta";
      };
      getNodeType = function(el, info) {
        if (info === "mirror") {
          return 'node-mirror';
        } else if (isSpecialIterationNode(info)) {
          return 'node-iteration';
        } else {
          return 'node-normal';
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<a href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
        if (info === "mirror") {
          labelValue += "<h3 class='node-name'>Mirror of " + el.operator + "</h3>";
        } else {
          labelValue += "<h3 class='node-name'>" + el.operator + "</h3>";
        }
        if (el.description === "") {
          labelValue += "";
        } else {
          stepName = el.description;
          stepName = shortenString(stepName);
          labelValue += "<h4 class='step-name'>" + stepName + "</h4>";
        }
        if (el.step_function != null) {
          labelValue += extendLabelNodeForIteration(el.id, maxW, maxH);
        } else {
          if (isSpecialIterationNode(info)) {
            labelValue += "<h5>" + info + " Node</h5>";
          }
          if (el.parallelism !== "") {
            labelValue += "<h5>Parallelism: " + el.parallelism + "</h5>";
          }
          if (el.operator !== undefined) {
            labelValue += "<h5>Operation: " + shortenString(el.operator_strategy) + "</h5>";
          }
        }
        labelValue += "</a>";
        return labelValue;
      };
      extendLabelNodeForIteration = function(id, maxW, maxH) {
        var labelValue, svgID;
        svgID = "svg-" + id;
        labelValue = "<svg class='" + svgID + "' width=" + maxW + " height=" + maxH + "><g /></svg>";
        return labelValue;
      };
      shortenString = function(s) {
        var sbr;
        if (s.charAt(0) === "<") {
          s = s.replace("<", "&lt;");
          s = s.replace(">", "&gt;");
        }
        sbr = "";
        while (s.length > 30) {
          sbr = sbr + s.substring(0, 30) + "<br>";
          s = s.substring(30, s.length);
        }
        sbr = sbr + s;
        return sbr;
      };
      createNode = function(g, data, el, isParent, maxW, maxH) {
        if (isParent == null) {
          isParent = false;
        }
        if (el.id === data.partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "partialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "partialSolution")
          });
        } else if (el.id === data.next_partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextPartialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextPartialSolution")
          });
        } else if (el.id === data.workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "workset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "workset")
          });
        } else if (el.id === data.next_workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextWorkset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextWorkset")
          });
        } else if (el.id === data.solution_set) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionSet", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionSet")
          });
        } else if (el.id === data.solution_delta) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionDelta", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionDelta")
          });
        } else {
          return g.setNode(el.id, {
            label: createLabelNode(el, "", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "")
          });
        }
      };
      createEdge = function(g, data, el, existingNodes, pred) {
        var missingNode;
        if (existingNodes.indexOf(pred.id) !== -1) {
          return g.setEdge(pred.id, el.id, {
            label: createLabelEdge(pred),
            labelType: 'html',
            arrowhead: 'normal'
          });
        } else {
          missingNode = searchForNode(data, pred.id);
          if (!(!missingNode || missingNode.alreadyAdded === true)) {
            missingNode.alreadyAdded = true;
            g.setNode(missingNode.id, {
              label: createLabelNode(missingNode, "mirror"),
              labelType: 'html',
              "class": getNodeType(missingNode, 'mirror')
            });
            return g.setEdge(missingNode.id, el.id, {
              label: createLabelEdge(missingNode),
              labelType: 'html'
            });
          }
        }
      };
      loadJsonToDagre = function(g, data) {
        var el, existingNodes, isParent, k, l, len, len1, maxH, maxW, pred, r, ref, sg, toIterate;
        existingNodes = [];
        if (data.nodes != null) {
          toIterate = data.nodes;
        } else {
          toIterate = data.step_function;
          isParent = true;
        }
        for (k = 0, len = toIterate.length; k < len; k++) {
          el = toIterate[k];
          maxW = 0;
          maxH = 0;
          if (el.step_function) {
            sg = new dagreD3.graphlib.Graph({
              multigraph: true,
              compound: true
            }).setGraph({
              nodesep: 20,
              edgesep: 0,
              ranksep: 20,
              rankdir: "LR",
              marginx: 10,
              marginy: 10
            });
            subgraphs[el.id] = sg;
            loadJsonToDagre(sg, el);
            r = new dagreD3.render();
            d3tmpSvg.select('g').call(r, sg);
            maxW = sg.graph().width;
            maxH = sg.graph().height;
            angular.element(mainTmpElement).empty();
          }
          createNode(g, data, el, isParent, maxW, maxH);
          existingNodes.push(el.id);
          if (el.inputs != null) {
            ref = el.inputs;
            for (l = 0, len1 = ref.length; l < len1; l++) {
              pred = ref[l];
              createEdge(g, data, el, existingNodes, pred);
            }
          }
        }
        return g;
      };
      searchForNode = function(data, nodeID) {
        var el, i, j;
        for (i in data.nodes) {
          el = data.nodes[i];
          if (el.id === nodeID) {
            return el;
          }
          if (el.step_function != null) {
            for (j in el.step_function) {
              if (el.step_function[j].id === nodeID) {
                return el.step_function[j];
              }
            }
          }
        }
      };
      drawGraph = function(data) {
        var g, i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
        g = new dagreD3.graphlib.Graph({
          multigraph: true,
          compound: true
        }).setGraph({
          nodesep: 70,
          edgesep: 0,
          ranksep: 50,
          rankdir: "LR",
          marginx: 40,
          marginy: 40
        });
        loadJsonToDagre(g, data);
        renderer = new dagreD3.render();
        d3mainSvgG.call(renderer, g);
        for (i in subgraphs) {
          sg = subgraphs[i];
          d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg);
        }
        newScale = 0.5;
        xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2);
        yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2);
        mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset]);
        d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")");
        mainZoom.on("zoom", function() {
          var ev;
          ev = d3.event;
          return d3mainSvgG.attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        });
        return mainZoom(d3mainSvg);
      };
      scope.$watch(attrs.plan, function(newPlan) {
        if (newPlan) {
          return drawGraph(newPlan);
        }
      });
    }
  };
}]);

angular.module('flinkApp').service('JobsService', ["$http", "flinkConfig", "$log", "amMoment", "$q", "$timeout", function($http, flinkConfig, $log, amMoment, $q, $timeout) {
  var currentJob, currentPlan, deferreds, jobObservers, jobs, notifyObservers;
  currentJob = null;
  currentPlan = null;
  deferreds = {};
  jobs = {
    running: [],
    finished: [],
    cancelled: [],
    failed: []
  };
  jobObservers = [];
  notifyObservers = function() {
    return angular.forEach(jobObservers, function(callback) {
      return callback();
    });
  };
  this.registerObserver = function(callback) {
    return jobObservers.push(callback);
  };
  this.unRegisterObserver = function(callback) {
    var index;
    index = jobObservers.indexOf(callback);
    return jobObservers.splice(index, 1);
  };
  this.stateList = function() {
    return ['SCHEDULED', 'DEPLOYING', 'RUNNING', 'FINISHED', 'FAILED', 'CANCELING', 'CANCELED'];
  };
  this.translateLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'finished':
        return 'success';
      case 'failed':
        return 'danger';
      case 'scheduled':
        return 'default';
      case 'deploying':
        return 'info';
      case 'running':
        return 'primary';
      case 'canceling':
        return 'warning';
      case 'pending':
        return 'info';
      case 'total':
        return 'black';
      default:
        return 'default';
    }
  };
  this.setEndTimes = function(list) {
    return angular.forEach(list, function(job, jobKey) {
      if (!(job['end-time'] > -1)) {
        return job['end-time'] = job['start-time'] + job['duration'];
      }
    });
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/joboverview").success((function(_this) {
      return function(data, status, headers, config) {
        angular.forEach(data, function(list, listKey) {
          switch (listKey) {
            case 'running':
              return jobs.running = _this.setEndTimes(list);
            case 'finished':
              return jobs.finished = _this.setEndTimes(list);
            case 'cancelled':
              return jobs.cancelled = _this.setEndTimes(list);
            case 'failed':
              return jobs.failed = _this.setEndTimes(list);
          }
        });
        deferred.resolve(jobs);
        return notifyObservers();
      };
    })(this));
    return deferred.promise;
  };
  this.getJobs = function(type) {
    return jobs[type];
  };
  this.getAllJobs = function() {
    return jobs;
  };
  this.loadJob = function(jobid) {
    currentJob = null;
    deferreds.job = $q.defer();
    $http.get(flinkConfig.jobServer + "/jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        return $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
          currentJob = data;
          return deferreds.job.resolve(data);
        });
      };
    })(this));
    return deferreds.job.promise;
  };
  this.loadPlan = function(jobid) {
    currentPlan = null;
    deferreds.plan = $q.defer();
    $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/plan").success(function(data) {
      currentPlan = data;
      return deferreds.plan.resolve(data);
    });
    return deferreds.plan.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var i, len, node, sub;
      for (i = 0, len = data.length; i < len; i++) {
        node = data[i];
        if (node.id === nodeid) {
          return node;
        }
        if (node.step_function) {
          sub = seekNode(nodeid, node.step_function);
        }
        if (sub) {
          return sub;
        }
      }
      return null;
    };
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentJob.plan.nodes);
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var i, len, ref, vertex;
    ref = currentJob.vertices;
    for (i = 0, len = ref.length; i < len; i++) {
      vertex = ref[i];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get(flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('OverviewController', ["$scope", "OverviewService", "JobsService", "$interval", "flinkConfig", function($scope, OverviewService, JobsService, $interval, flinkConfig) {
  var refresh;
  $scope.jobObserver = function() {
    $scope.runningJobs = JobsService.getJobs('running');
    return $scope.finishedJobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  $scope.jobObserver();
  OverviewService.loadOverview().then(function(data) {
    return $scope.overview = data;
  });
  refresh = $interval(function() {
    return OverviewService.loadOverview().then(function(data) {
      return $scope.overview = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('OverviewService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var overview;
  overview = {};
  this.loadOverview = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmN0cmwuanMiLCJtb2R1bGVzL2pvYnMvam9icy5kaXIuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxPQUFPLFlBQVksQ0FBQyxhQUFhLGtCQUl4QyxtQkFBSSxTQUFDLFlBQUQ7RUFDSCxXQUFXLGlCQUFpQjtFQ3JCNUIsT0RzQkEsV0FBVyxjQUFjLFdBQUE7SUFDdkIsV0FBVyxpQkFBaUIsQ0FBQyxXQUFXO0lDckJ4QyxPRHNCQSxXQUFXLGVBQWU7O0lBSTdCLE1BQU0sZUFBZTtFQUNwQixXQUFXO0VBRVgsb0JBQW9CO0dBS3JCLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUM1QkgsT0Q2QkEsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUM3QlosT0QrQkEsVUFBVSxXQUFBO01DOUJSLE9EK0JBLFlBQVk7T0FDWixZQUFZOztJQUtqQixpQ0FBTyxT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QVE7O0VBRTVDLFlBQVksaUJBQWlCLE9BQU87RUFDcEMsT0FBTyxJQUFJLFlBQVksV0FBQTtJQ2xCckIsT0RtQkEsWUFBWSxtQkFBbUIsT0FBTzs7RUFFeEMsT0FBTztFQUVQLGdCQUFnQixlQUFlLEtBQUssU0FBQyxNQUFEO0lDbkJsQyxPRG9CQSxPQUFPLFdBQVc7O0VBRXBCLFVBQVUsVUFBVSxXQUFBO0lDbkJsQixPRG9CQSxnQkFBZ0IsZUFBZSxLQUFLLFNBQUMsTUFBRDtNQ25CbEMsT0RvQkEsT0FBTyxXQUFXOztLQUNwQixZQUFZO0VDbEJkLE9Eb0JBLE9BQU8sSUFBSSxZQUFZLFdBQUE7SUNuQnJCLE9Eb0JBLFVBQVUsT0FBTzs7O0FDakJyQjtBQ0xBLFFBQVEsT0FBTyxZQUVkLFFBQVEsa0RBQW1CLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBQzFCLElBQUE7RUFBQSxXQUFXO0VBRVgsS0FBQyxlQUFlLFdBQUE7SUFDZCxJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxhQUNqQyxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUFDUCxXQUFXO01DcEJYLE9EcUJBLFNBQVMsUUFBUTs7SUNuQm5CLE9EcUJBLFNBQVM7O0VDbkJYLE9EcUJBOztBQ25CRiIsImZpbGUiOiJpbmRleC5qcyIsInNvdXJjZXNDb250ZW50IjpbIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnJ1biAoJHJvb3RTY29wZSkgLT5cbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlXG4gICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSAtPlxuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZVxuICAgICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnZhbHVlICdmbGlua0NvbmZpZycsIHtcbiAgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDo4MDgxJ1xuIyAgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDozMDAwL25ldy1zZXJ2ZXInXG4gIFwicmVmcmVzaC1pbnRlcnZhbFwiOiAxMDAwMFxufVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKEpvYnNTZXJ2aWNlLCBNYWluU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cbiAgTWFpblNlcnZpY2UubG9hZENvbmZpZygpLnRoZW4gKGNvbmZpZykgLT5cbiAgICBhbmd1bGFyLmV4dGVuZCBmbGlua0NvbmZpZywgY29uZmlnXG5cbiAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXG5cbiAgICAkaW50ZXJ2YWwgLT5cbiAgICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKClcbiAgICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbmZpZyAoJHVpVmlld1Njcm9sbFByb3ZpZGVyKSAtPlxuICAkdWlWaWV3U2Nyb2xsUHJvdmlkZXIudXNlQW5jaG9yU2Nyb2xsKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSAtPlxuICAkc3RhdGVQcm92aWRlci5zdGF0ZSBcIm92ZXJ2aWV3XCIsXG4gICAgdXJsOiBcIi9vdmVydmlld1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJydW5uaW5nLWpvYnNcIixcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgXG4gIC5zdGF0ZSBcImNvbXBsZXRlZC1qb2JzXCIsXG4gICAgdXJsOiBcIi9jb21wbGV0ZWQtam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYlwiLFxuICAgIHVybDogXCIvam9icy97am9iaWR9XCJcbiAgICBhYnN0cmFjdDogdHJ1ZVxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVKb2JDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhblwiLFxuICAgIHVybDogXCJcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Db250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5ub2RlXCIsXG4gICAgdXJsOiBcIi92ZXJ0ZXgve25vZGVpZH1cIlxuICAgIGFic3RyYWN0OiB0cnVlXG4gICAgdmlld3M6XG4gICAgICAnbm9kZS10YWJzJzpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLXRhYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZVRhYnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5ub2RlLmdlbmVyaWNcIixcbiAgICB1cmw6IFwiXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk5vZGVMaXN0Q29udHJvbGxlcicgXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0Lmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5ub2RlLnByb3BlcnRpZXNcIixcbiAgICB1cmw6IFwiL3Byb3BlcnRpZXNcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZVByb3BlcnRpZXNDb250cm9sbGVyJ1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUucHJvcGVydGllcy5odG1sXCJcblxuICAjIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5ub2RlLmFjY3VtdWxhdG9yc1wiLFxuICAjICAgdXJsOiBcIi9hY2N1bXVsYXRvcnNcIlxuICAjICAgdmlld3M6XG4gICMgICAgICdub2RlLWNvbnRlbnRzJzpcbiAgIyAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk5vZGVBY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xuICAjICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5hY2N1bXVsYXRvcnMuaHRtbFwiXG5cbiAgIyAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4ubm9kZVwiLFxuICAjICAgdXJsOiBcIi92ZXJ0ZXgve25vZGVpZH1cIlxuICAjICAgYWJzdHJhY3Q6IHRydWVcbiAgIyAgIHZpZXdzOlxuICAjICAgICBub2RlOlxuICAjICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5odG1sXCJcbiAgIyAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk5vZGVDb250cm9sbGVyJ1xuXG4gICMgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLm5vZGUuc3VidGFza3NcIixcbiAgIyAgIHVybDogXCJcIlxuICAjICAgdmlld3M6XG4gICMgICAgIG5vZGVUYWI6XG4gICMgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLnN1YnRhc2tzLmh0bWxcIlxuXG4gICMgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLm5vZGUucHJvcGVydGllc1wiLFxuICAjICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCJcbiAgIyAgIHZpZXdzOlxuICAjICAgICBub2RlVGFiOlxuICAjICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5wcm9wZXJ0aWVzLmh0bWxcIlxuXG4gICMgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLm5vZGUuYWNjdW11bGF0b3JzXCIsXG4gICMgICB1cmw6IFwiL3Byb3BlcnRpZXNcIlxuICAjICAgdmlld3M6XG4gICMgICAgIG5vZGVUYWI6XG4gICMgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmFjY3VtdWxhdG9ycy5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsXG4gICAgdXJsOiBcIi90aW1lbGluZVwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLFxuICAgIHVybDogXCIve3ZlcnRleElkfVwiXG4gICAgdmlld3M6XG4gICAgICB2ZXJ0ZXg6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnN0YXRpc3RpY3NcIixcbiAgICB1cmw6IFwiL3N0YXRpc3RpY3NcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2Iuc3RhdGlzdGljcy5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIixcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IuY29uZmlnXCIsXG4gICAgdXJsOiBcIi9jb25maWdcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuXG4gICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UgXCIvb3ZlcnZpZXdcIlxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJywgWyd1aS5yb3V0ZXInLCAnYW5ndWxhck1vbWVudCddKS5ydW4oZnVuY3Rpb24oJHJvb3RTY29wZSkge1xuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2U7XG4gIHJldHVybiAkcm9vdFNjb3BlLnNob3dTaWRlYmFyID0gZnVuY3Rpb24oKSB7XG4gICAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9ICEkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlO1xuICAgIHJldHVybiAkcm9vdFNjb3BlLnNpZGViYXJDbGFzcyA9ICdmb3JjZS1zaG93JztcbiAgfTtcbn0pLnZhbHVlKCdmbGlua0NvbmZpZycsIHtcbiAgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDo4MDgxJyxcbiAgXCJyZWZyZXNoLWludGVydmFsXCI6IDEwMDAwXG59KS5ydW4oZnVuY3Rpb24oSm9ic1NlcnZpY2UsIE1haW5TZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSB7XG4gIHJldHVybiBNYWluU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihjb25maWcpIHtcbiAgICBhbmd1bGFyLmV4dGVuZChmbGlua0NvbmZpZywgY29uZmlnKTtcbiAgICBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIHJldHVybiAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICB9KTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkdWlWaWV3U2Nyb2xsUHJvdmlkZXIpIHtcbiAgcmV0dXJuICR1aVZpZXdTY3JvbGxQcm92aWRlci51c2VBbmNob3JTY3JvbGwoKTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSB7XG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlKFwib3ZlcnZpZXdcIiwge1xuICAgIHVybDogXCIvb3ZlcnZpZXdcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInJ1bm5pbmctam9ic1wiLCB7XG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImNvbXBsZXRlZC1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYlwiLCB7XG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIixcbiAgICBhYnN0cmFjdDogdHJ1ZSxcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVKb2JDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW5cIiwge1xuICAgIHVybDogXCJcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Db250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4ubm9kZVwiLCB7XG4gICAgdXJsOiBcIi92ZXJ0ZXgve25vZGVpZH1cIixcbiAgICBhYnN0cmFjdDogdHJ1ZSxcbiAgICB2aWV3czoge1xuICAgICAgJ25vZGUtdGFicyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLXRhYnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk5vZGVUYWJzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLm5vZGUuZ2VuZXJpY1wiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk5vZGVMaXN0Q29udHJvbGxlcicsXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0Lmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4ubm9kZS5wcm9wZXJ0aWVzXCIsIHtcbiAgICB1cmw6IFwiL3Byb3BlcnRpZXNcIixcbiAgICB2aWV3czoge1xuICAgICAgJ25vZGUtZGV0YWlscyc6IHtcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Ob2RlUHJvcGVydGllc0NvbnRyb2xsZXInLFxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUucHJvcGVydGllcy5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZVwiLCB7XG4gICAgdXJsOiBcIi90aW1lbGluZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIHZlcnRleDoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnN0YXRpc3RpY3NcIiwge1xuICAgIHVybDogXCIvc3RhdGlzdGljc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnN0YXRpc3RpY3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLCB7XG4gICAgdXJsOiBcIi9leGNlcHRpb25zXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JFeGNlcHRpb25zQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSk7XG4gIHJldHVybiAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlKFwiL292ZXJ2aWV3XCIpO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2JzTGFiZWwnLCAoSm9ic1NlcnZpY2UpIC0+XG4gIHRyYW5zY2x1ZGU6IHRydWVcbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6IFwiQFwiXG5cbiAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCJcbiAgXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XG4gICAgICAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnaW5kaWNhdG9yUHJpbWFyeScsIChKb2JzU2VydmljZSkgLT5cbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6ICdAJ1xuXG4gIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RhYmxlUHJvcGVydHknLCAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOlxuICAgIHZhbHVlOiAnPSdcblxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ2JzTGFiZWwnLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHRyYW5zY2x1ZGU6IHRydWUsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6IFwiQFwiXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdpbmRpY2F0b3JQcmltYXJ5JywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogJ0AnXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0YWJsZVByb3BlcnR5JywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiB7XG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgdmFsdWU6ICc9J1xuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHRkIHRpdGxlPVxcXCJ7e3ZhbHVlIHx8ICdOb25lJ319XFxcIj57e3ZhbHVlIHx8ICdOb25lJ319PC90ZD5cIlxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmZpbHRlciBcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCAoYW5ndWxhck1vbWVudENvbmZpZykgLT5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSAtPlxuICAgIHJldHVybiBcIlwiICBpZiB0eXBlb2YgdmFsdWUgaXMgXCJ1bmRlZmluZWRcIiBvciB2YWx1ZSBpcyBudWxsXG5cbiAgICBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7IHRyaW06IGZhbHNlIH0pXG5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzXG5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyXG5cbi5maWx0ZXIgXCJodW1hbml6ZVRhc2tOYW1lXCIsIC0+XG4gICh0ZXh0KSAtPlxuICAgICMgVE9ETzogZXh0ZW5kLi4uIGEgbG90XG4gICAgaWYgdGV4dCB0aGVuIHRleHQucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpLnJlcGxhY2UoXCI8YnIvPlwiLFwiXCIpIGVsc2UgJydcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmZpbHRlcihcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCBmdW5jdGlvbihhbmd1bGFyTW9tZW50Q29uZmlnKSB7XG4gIHZhciBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9IGZ1bmN0aW9uKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSB7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIHJldHVybiBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7XG4gICAgICB0cmltOiBmYWxzZVxuICAgIH0pO1xuICB9O1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnM7XG4gIHJldHVybiBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG59KS5maWx0ZXIoXCJodW1hbml6ZVRhc2tOYW1lXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIGlmICh0ZXh0KSB7XG4gICAgICByZXR1cm4gdGV4dC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIikucmVwbGFjZShcIjxici8+XCIsIFwiXCIpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gJyc7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ01haW5TZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIGpvYk9ic2VydmVycyA9IFtdXG5cbiAgQGxvYWRDb25maWcgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2NvbmZpZ1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnTWFpblNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBqb2JPYnNlcnZlcnM7XG4gIGpvYk9ic2VydmVycyA9IFtdO1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9jb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcblxuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG5cbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSkgLT5cbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkXG4gICRyb290U2NvcGUuam9iID0gbnVsbFxuICAkcm9vdFNjb3BlLnBsYW4gPSBudWxsXG5cbiAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHJvb3RTY29wZS5qb2IgPSBkYXRhXG4gICAgJHJvb3RTY29wZS5wbGFuID0gZGF0YS5wbGFuXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRyb290U2NvcGUuam9iID0gbnVsbFxuICAgICRyb290U2NvcGUucGxhbiA9IG51bGxcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbkNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIGNvbnNvbGUubG9nICdKb2JQbGFuQ29udHJvbGxlcidcblxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbk5vZGVDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkYW5jaG9yU2Nyb2xsKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbk5vZGVDb250cm9sbGVyJ1xuXG4gICRzY29wZS5ub2RlaWQgPSAkc3RhdGVQYXJhbXMubm9kZWlkXG5cbiAgJHNjb3BlLnN0YXRlTGlzdCA9IEpvYnNTZXJ2aWNlLnN0YXRlTGlzdCgpXG5cbiAgSm9ic1NlcnZpY2UuZ2V0Tm9kZSgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5ub2RlID0gZGF0YVxuICAgICRzY29wZS5qb2IuY3VycmVudE5vZGUgPSBkYXRhXG5cbiAgICAjIGFuY2hvciA9IFwidmVydGV4LXJvdy1cIiArIGRhdGEuaWRcbiAgICAjIGNvbnNvbGUubG9nIGFuY2hvclxuICAgICMgJGFuY2hvclNjcm9sbChhbmNob3IpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5Ob2RlVGFic0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbk5vZGVUYWJzQ29udHJvbGxlcidcblxuICAkc2NvcGUubm9kZWlkID0gJHN0YXRlUGFyYW1zLm5vZGVpZFxuXG4gIEpvYnNTZXJ2aWNlLmdldE5vZGUoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUubm9kZSA9IGRhdGFcbiAgICAkc2NvcGUuam9iLmN1cnJlbnROb2RlID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuTm9kZUxpc3RDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5Ob2RlTGlzdENvbnRyb2xsZXInXG5cbiAgIyAkc2NvcGUubm9kZWlkID0gJHN0YXRlUGFyYW1zLm5vZGVpZFxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbk5vZGVQcm9wZXJ0aWVzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5Ob2RlUHJvcGVydGllc0NvbnRyb2xsZXInXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5Ob2RlQWNjdW11bGF0b3JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5Ob2RlQWNjdW11bGF0b3JzQ29udHJvbGxlcidcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS52ZXJ0ZXggPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLmV4Y2VwdGlvbnMgPSBkYXRhXG5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZUpvYkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSkge1xuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWQ7XG4gICRyb290U2NvcGUuam9iID0gbnVsbDtcbiAgJHJvb3RTY29wZS5wbGFuID0gbnVsbDtcbiAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICRyb290U2NvcGUuam9iID0gZGF0YTtcbiAgICByZXR1cm4gJHJvb3RTY29wZS5wbGFuID0gZGF0YS5wbGFuO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgJHJvb3RTY29wZS5qb2IgPSBudWxsO1xuICAgIHJldHVybiAkcm9vdFNjb3BlLnBsYW4gPSBudWxsO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5Db250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5Db250cm9sbGVyJyk7XG4gIHJldHVybiAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuTm9kZUNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJGFuY2hvclNjcm9sbCkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhbk5vZGVDb250cm9sbGVyJyk7XG4gICRzY29wZS5ub2RlaWQgPSAkc3RhdGVQYXJhbXMubm9kZWlkO1xuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG4gIHJldHVybiBKb2JzU2VydmljZS5nZXROb2RlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICRzY29wZS5ub2RlID0gZGF0YTtcbiAgICByZXR1cm4gJHNjb3BlLmpvYi5jdXJyZW50Tm9kZSA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbk5vZGVUYWJzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhbk5vZGVUYWJzQ29udHJvbGxlcicpO1xuICAkc2NvcGUubm9kZWlkID0gJHN0YXRlUGFyYW1zLm5vZGVpZDtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE5vZGUoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgJHNjb3BlLm5vZGUgPSBkYXRhO1xuICAgIHJldHVybiAkc2NvcGUuam9iLmN1cnJlbnROb2RlID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuTm9kZUxpc3RDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQbGFuTm9kZUxpc3RDb250cm9sbGVyJyk7XG4gIHJldHVybiAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuTm9kZVByb3BlcnRpZXNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIGNvbnNvbGUubG9nKCdKb2JQbGFuTm9kZVByb3BlcnRpZXNDb250cm9sbGVyJyk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuTm9kZUFjY3VtdWxhdG9yc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gY29uc29sZS5sb2coJ0pvYlBsYW5Ob2RlQWNjdW11bGF0b3JzQ29udHJvbGxlcicpO1xufSkuY29udHJvbGxlcignSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUudmVydGV4ID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JFeGNlcHRpb25zQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGE7XG4gIH0pO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3ZlcnRleCcsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcblxuICBzY29wZTpcbiAgICBkYXRhOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICB0ZXN0RGF0YSA9IFtdXG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLnN1YnRhc2tzLCAoc3VidGFzaywgaSkgLT5cbiAgICAgICAgdGVzdERhdGEucHVzaCB7XG4gICAgICAgICAgbGFiZWw6IFwiI3tzdWJ0YXNrLmhvc3R9ICgje3N1YnRhc2suc3VidGFza30pXCJcbiAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIlxuICAgICAgICAgICAgICBjb2xvcjogXCIjNjY2XCJcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlNDSEVEVUxFRFwiXVxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIlxuICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCJcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXVxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIlxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl1cbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9XG4gICAgICAgICAgXVxuICAgICAgICB9XG5cbiAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpXG4gICAgICAudGlja0Zvcm1hdCh7XG4gICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKVxuICAgICAgICAjIHRpY2tJbnRlcnZhbDogMVxuICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgfSlcbiAgICAgIC5wcmVmaXgoXCJzaW5nbGVcIilcbiAgICAgIC5sYWJlbEZvcm1hdCgobGFiZWwpIC0+XG4gICAgICAgIGxhYmVsXG4gICAgICApXG4gICAgICAubWFyZ2luKHsgbGVmdDogMTAwLCByaWdodDogMCwgdG9wOiAwLCBib3R0b206IDAgfSlcbiAgICAgIC5pdGVtSGVpZ2h0KDMwKVxuICAgICAgLnJlbGF0aXZlVGltZSgpXG5cbiAgICAgIHN2ZyA9IGQzLnNlbGVjdChzdmdFbClcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcbiAgICAgIC5jYWxsKGNoYXJ0KVxuXG4gICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSlcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndGltZWxpbmUnLCAoJHN0YXRlKSAtPlxuICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcblxuICBzY29wZTpcbiAgICBqb2I6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxuXG4gICAgdHJhbnNsYXRlTGFiZWwgPSAobGFiZWwpIC0+XG4gICAgICBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIilcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICB0ZXN0RGF0YSA9IFtdXG5cbiAgICAgIHRlc3REYXRhLnB1c2ggXG4gICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCJcbiAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCJcbiAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICBzdGFydGluZ190aW1lOiBkYXRhLnRpbWVzdGFtcHNbXCJDUkVBVEVEXCJdXG4gICAgICAgICAgZW5kaW5nX3RpbWU6IGRhdGEudGltZXN0YW1wc1tcIkNSRUFURURcIl0gKyAxXG4gICAgICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcbiAgICAgICAgXVxuXG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLnZlcnRpY2VzLCAodmVydGV4KSAtPlxuICAgICAgICBpZiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSA+IC0xXG4gICAgICAgICAgdGVzdERhdGEucHVzaCBcbiAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIlxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXG4gICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIF1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soKGQsIGksIGRhdHVtKSAtPlxuICAgICAgICBpZiBkLmxpbmtcbiAgICAgICAgICAkc3RhdGUuZ28gXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7IGpvYmlkOiBkYXRhLmppZCwgdmVydGV4SWQ6IGQubGluayB9XG5cbiAgICAgIClcbiAgICAgIC50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXG4gICAgICAgICMgdGlja1RpbWU6IGQzLnRpbWUuc2Vjb25kXG4gICAgICAgICMgdGlja0ludGVydmFsOiAwLjVcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pXG4gICAgICAucHJlZml4KFwibWFpblwiKVxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuICAgICAgLml0ZW1IZWlnaHQoMzApXG4gICAgICAuc2hvd0JvcmRlckxpbmUoKVxuICAgICAgLnNob3dIb3VyVGltZWxpbmUoKVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5qb2IsIChkYXRhKSAtPlxuICAgICAgYW5hbHl6ZVRpbWUoZGF0YSkgaWYgZGF0YVxuXG4gICAgcmV0dXJuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdqb2JQbGFuJywgKCR0aW1lb3V0KSAtPlxuICB0ZW1wbGF0ZTogXCJcbiAgICA8c3ZnIGNsYXNzPSdncmFwaCcgd2lkdGg9JzUwMCcgaGVpZ2h0PSc0MDAnPjxnIC8+PC9zdmc+XG4gICAgPHN2ZyBjbGFzcz0ndG1wJyB3aWR0aD0nMScgaGVpZ2h0PScxJz48ZyAvPjwvc3ZnPlxuICAgIDxkaXYgY2xhc3M9J2J0bi1ncm91cCB6b29tLWJ1dHRvbnMnPlxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT5cbiAgICAgIDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1vdXQnIG5nLWNsaWNrPSd6b29tT3V0KCknPjxpIGNsYXNzPSdmYSBmYS1taW51cycgLz48L2E+XG4gICAgPC9kaXY+XCJcblxuICBzY29wZTpcbiAgICBwbGFuOiAnPSdcblxuICBsaW5rOiAoc2NvcGUsIGVsZW0sIGF0dHJzKSAtPlxuICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXG4gICAgc3ViZ3JhcGhzID0gW11cbiAgICBqb2JpZCA9IGF0dHJzLmpvYmlkXG5cbiAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF1cbiAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXVxuXG4gICAgZDNtYWluU3ZnID0gZDMuc2VsZWN0KG1haW5TdmdFbGVtZW50KVxuICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpXG4gICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpXG5cbiAgICAjIGFuZ3VsYXIuZWxlbWVudChtYWluRykuZW1wdHkoKVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChlbGVtLmNoaWxkcmVuKClbMF0pLndpZHRoKGNvbnRhaW5lclcpXG5cbiAgICBzY29wZS56b29tSW4gPSAtPlxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA8IDIuOTlcbiAgICAgICAgXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIHpvb20gb2JqZWN0XG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIG1haW5ab29tLnNjYWxlIG1haW5ab29tLnNjYWxlKCkgKyAwLjFcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cbiAgICAgICAgXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCJcblxuICAgIHNjb3BlLnpvb21PdXQgPSAtPlxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA+IDAuMzFcbiAgICAgICAgXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIG1haW5ab29tIG9iamVjdFxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpIC0gMC4xXG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXG4gICAgICAgIFxuICAgICAgICAjIFRyYW5zZm9ybSBzdmdcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXG5cbiAgICAjY3JlYXRlIGEgbGFiZWwgb2YgYW4gZWRnZVxuICAgIGNyZWF0ZUxhYmVsRWRnZSA9IChlbCkgLT5cbiAgICAgIGxhYmVsVmFsdWUgPSBcIlwiXG4gICAgICBpZiBlbC5zaGlwX3N0cmF0ZWd5PyBvciBlbC5sb2NhbF9zdHJhdGVneT9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZWwuc2hpcF9zdHJhdGVneSAgaWYgZWwuc2hpcF9zdHJhdGVneT9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIiAgdW5sZXNzIGVsLnRlbXBfbW9kZSBpcyBgdW5kZWZpbmVkYFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5ICB1bmxlc3MgZWwubG9jYWxfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiXG4gICAgICBsYWJlbFZhbHVlXG5cblxuICAgICMgdHJ1ZSwgaWYgdGhlIG5vZGUgaXMgYSBzcGVjaWFsIG5vZGUgZnJvbSBhbiBpdGVyYXRpb25cbiAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gKGluZm8pIC0+XG4gICAgICAoaW5mbyBpcyBcInBhcnRpYWxTb2x1dGlvblwiIG9yIGluZm8gaXMgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIndvcmtzZXRcIiBvciBpbmZvIGlzIFwibmV4dFdvcmtzZXRcIiBvciBpbmZvIGlzIFwic29sdXRpb25TZXRcIiBvciBpbmZvIGlzIFwic29sdXRpb25EZWx0YVwiKVxuXG4gICAgZ2V0Tm9kZVR5cGUgPSAoZWwsIGluZm8pIC0+XG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgJ25vZGUtbWlycm9yJ1xuXG4gICAgICBlbHNlIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgJ25vZGUtaXRlcmF0aW9uJ1xuXG4gICAgICBlbHNlXG4gICAgICAgICAgJ25vZGUtbm9ybWFsJ1xuICAgICAgXG4gICAgIyBjcmVhdGVzIHRoZSBsYWJlbCBvZiBhIG5vZGUsIGluIGluZm8gaXMgc3RvcmVkLCB3aGV0aGVyIGl0IGlzIGEgc3BlY2lhbCBub2RlIChsaWtlIGEgbWlycm9yIGluIGFuIGl0ZXJhdGlvbilcbiAgICBjcmVhdGVMYWJlbE5vZGUgPSAoZWwsIGluZm8sIG1heFcsIG1heEgpIC0+XG4gICAgICBsYWJlbFZhbHVlID0gXCI8YSBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXG5cbiAgICAgICMgTm9kZW5hbWVcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiXG4gICAgICBlbHNlXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgaWYgZWwuZGVzY3JpcHRpb24gaXMgXCJcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCJcbiAgICAgIGVsc2VcbiAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvblxuICAgICAgICBcbiAgICAgICAgIyBjbGVhbiBzdGVwTmFtZVxuICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIlxuICAgICAgXG4gICAgICAjIElmIHRoaXMgbm9kZSBpcyBhbiBcIml0ZXJhdGlvblwiIHdlIG5lZWQgYSBkaWZmZXJlbnQgcGFuZWwtYm9keVxuICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpXG4gICAgICBlbHNlXG4gICAgICAgIFxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3MgICAgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCIgIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIiAgdW5sZXNzIGVsLnBhcmFsbGVsaXNtIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5vcGVyYXRvciBpcyBgdW5kZWZpbmVkYFxuICAgICAgXG4gICAgICBsYWJlbFZhbHVlICs9IFwiPC9hPlwiXG4gICAgICBsYWJlbFZhbHVlXG5cbiAgICAjIEV4dGVuZHMgdGhlIGxhYmVsIG9mIGEgbm9kZSB3aXRoIGFuIGFkZGl0aW9uYWwgc3ZnIEVsZW1lbnQgdG8gcHJlc2VudCB0aGUgaXRlcmF0aW9uLlxuICAgIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiA9IChpZCwgbWF4VywgbWF4SCkgLT5cbiAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZFxuXG4gICAgICBsYWJlbFZhbHVlID0gXCI8c3ZnIGNsYXNzPSdcIiArIHN2Z0lEICsgXCInIHdpZHRoPVwiICsgbWF4VyArIFwiIGhlaWdodD1cIiArIG1heEggKyBcIj48ZyAvPjwvc3ZnPlwiXG4gICAgICBsYWJlbFZhbHVlXG5cbiAgICAjIFNwbGl0IGEgc3RyaW5nIGludG8gbXVsdGlwbGUgbGluZXMgc28gdGhhdCBlYWNoIGxpbmUgaGFzIGxlc3MgdGhhbiAzMCBsZXR0ZXJzLlxuICAgIHNob3J0ZW5TdHJpbmcgPSAocykgLT5cbiAgICAgICMgbWFrZSBzdXJlIHRoYXQgbmFtZSBkb2VzIG5vdCBjb250YWluIGEgPCAoYmVjYXVzZSBvZiBodG1sKVxuICAgICAgaWYgcy5jaGFyQXQoMCkgaXMgXCI8XCJcbiAgICAgICAgcyA9IHMucmVwbGFjZShcIjxcIiwgXCImbHQ7XCIpXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI+XCIsIFwiJmd0O1wiKVxuICAgICAgc2JyID0gXCJcIlxuICAgICAgd2hpbGUgcy5sZW5ndGggPiAzMFxuICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIlxuICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKVxuICAgICAgc2JyID0gc2JyICsgc1xuICAgICAgc2JyXG5cbiAgICBjcmVhdGVOb2RlID0gKGcsIGRhdGEsIGVsLCBpc1BhcmVudCA9IGZhbHNlLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgIyBjcmVhdGUgbm9kZSwgc2VuZCBhZGRpdGlvbmFsIGluZm9ybWF0aW9ucyBhYm91dCB0aGUgbm9kZSBpZiBpdCBpcyBhIHNwZWNpYWwgb25lXG4gICAgICBpZiBlbC5pZCBpcyBkYXRhLnBhcnRpYWxfc29sdXRpb25cbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3BhcnRpYWxfc29sdXRpb25cbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLndvcmtzZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwid29ya3NldFwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLm5leHRfd29ya3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0V29ya3NldFwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5zb2x1dGlvbl9zZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25TZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fZGVsdGFcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25EZWx0YVwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcblxuICAgICAgZWxzZVxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJcIilcblxuICAgIGNyZWF0ZUVkZ2UgPSAoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpIC0+XG4gICAgICB1bmxlc3MgZXhpc3RpbmdOb2Rlcy5pbmRleE9mKHByZWQuaWQpIGlzIC0xXG4gICAgICAgIGcuc2V0RWRnZSBwcmVkLmlkLCBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKHByZWQpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBhcnJvd2hlYWQ6ICdub3JtYWwnXG5cbiAgICAgIGVsc2VcbiAgICAgICAgbWlzc2luZ05vZGUgPSBzZWFyY2hGb3JOb2RlKGRhdGEsIHByZWQuaWQpXG4gICAgICAgIHVubGVzcyAhbWlzc2luZ05vZGUgb3IgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkIGlzIHRydWVcbiAgICAgICAgICBtaXNzaW5nTm9kZS5hbHJlYWR5QWRkZWQgPSB0cnVlXG4gICAgICAgICAgZy5zZXROb2RlIG1pc3NpbmdOb2RlLmlkLFxuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShtaXNzaW5nTm9kZSwgXCJtaXJyb3JcIilcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUobWlzc2luZ05vZGUsICdtaXJyb3InKVxuXG4gICAgICAgICAgZy5zZXRFZGdlIG1pc3NpbmdOb2RlLmlkLCBlbC5pZCxcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuXG4gICAgbG9hZEpzb25Ub0RhZ3JlID0gKGcsIGRhdGEpIC0+XG4gICAgICBleGlzdGluZ05vZGVzID0gW11cblxuICAgICAgaWYgZGF0YS5ub2Rlcz9cbiAgICAgICAgIyBUaGlzIGlzIHRoZSBub3JtYWwganNvbiBkYXRhXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXNcblxuICAgICAgZWxzZVxuICAgICAgICAjIFRoaXMgaXMgYW4gaXRlcmF0aW9uLCB3ZSBub3cgc3RvcmUgc3BlY2lhbCBpdGVyYXRpb24gbm9kZXMgaWYgcG9zc2libGVcbiAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIGlzUGFyZW50ID0gdHJ1ZVxuXG4gICAgICBmb3IgZWwgaW4gdG9JdGVyYXRlXG4gICAgICAgIG1heFcgPSAwXG4gICAgICAgIG1heEggPSAwXG5cbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvblxuICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICBub2Rlc2VwOiAyMFxuICAgICAgICAgICAgZWRnZXNlcDogMFxuICAgICAgICAgICAgcmFua3NlcDogMjBcbiAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICAgICAgbWFyZ2lueDogMTBcbiAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KVxuXG4gICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnXG5cbiAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKVxuXG4gICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXG4gICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZylcbiAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aFxuICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodFxuXG4gICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpXG5cbiAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpXG5cbiAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoIGVsLmlkXG4gICAgICAgIFxuICAgICAgICAjIGNyZWF0ZSBlZGdlcyBmcm9tIGlucHV0cyB0byBjdXJyZW50IG5vZGVcbiAgICAgICAgaWYgZWwuaW5wdXRzP1xuICAgICAgICAgIGZvciBwcmVkIGluIGVsLmlucHV0c1xuICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZClcblxuICAgICAgZ1xuXG4gICAgIyBzZWFyY2hlcyBpbiB0aGUgZ2xvYmFsIEpTT05EYXRhIGZvciB0aGUgbm9kZSB3aXRoIHRoZSBnaXZlbiBpZFxuICAgIHNlYXJjaEZvck5vZGUgPSAoZGF0YSwgbm9kZUlEKSAtPlxuICAgICAgZm9yIGkgb2YgZGF0YS5ub2Rlc1xuICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV1cbiAgICAgICAgcmV0dXJuIGVsICBpZiBlbC5pZCBpcyBub2RlSURcbiAgICAgICAgXG4gICAgICAgICMgbG9vayBmb3Igbm9kZXMgdGhhdCBhcmUgaW4gaXRlcmF0aW9uc1xuICAgICAgICBpZiBlbC5zdGVwX2Z1bmN0aW9uP1xuICAgICAgICAgIGZvciBqIG9mIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdICBpZiBlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkIGlzIG5vZGVJRFxuXG4gICAgZHJhd0dyYXBoID0gKGRhdGEpIC0+XG4gICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XG4gICAgICAgIG5vZGVzZXA6IDcwXG4gICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgcmFua3NlcDogNTBcbiAgICAgICAgcmFua2RpcjogXCJMUlwiXG4gICAgICAgIG1hcmdpbng6IDQwXG4gICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pXG5cbiAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKVxuXG4gICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXG4gICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpXG5cbiAgICAgIGZvciBpLCBzZyBvZiBzdWJncmFwaHNcbiAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKVxuXG4gICAgICBuZXdTY2FsZSA9IDAuNVxuXG4gICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMilcbiAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpXG5cbiAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKVxuXG4gICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIilcblxuICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIC0+XG4gICAgICAgIGV2ID0gZDMuZXZlbnRcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlICsgXCIpIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIilcIlxuICAgICAgKVxuICAgICAgbWFpblpvb20oZDNtYWluU3ZnKVxuXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnBsYW4sIChuZXdQbGFuKSAtPlxuICAgICAgZHJhd0dyYXBoKG5ld1BsYW4pIGlmIG5ld1BsYW5cblxuICAgIHJldHVyblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZGlyZWN0aXZlKCd2ZXJ0ZXgnLCBmdW5jdGlvbigkc3RhdGUpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBkYXRhOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBjaGFydCwgc3ZnLCB0ZXN0RGF0YTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEuc3VidGFza3MsIGZ1bmN0aW9uKHN1YnRhc2ssIGkpIHtcbiAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICBsYWJlbDogc3VidGFzay5ob3N0ICsgXCIgKFwiICsgc3VidGFzay5zdWJ0YXNrICsgXCIpXCIsXG4gICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCIsXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiIzY2NlwiLFxuICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl0sXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICAgIGxhYmVsOiBcIkRlcGxveWluZ1wiLFxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIixcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdLFxuICAgICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgICB9LCB7XG4gICAgICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiLFxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIixcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0sXG4gICAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJzaW5nbGVcIikubGFiZWxGb3JtYXQoZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgICByZXR1cm4gbGFiZWw7XG4gICAgICAgIH0pLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMTAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSkuaXRlbUhlaWdodCgzMCkucmVsYXRpdmVUaW1lKCk7XG4gICAgICAgIHJldHVybiBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZShzY29wZS5kYXRhKTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RpbWVsaW5lJywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBqb2I6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWwsIHRyYW5zbGF0ZUxhYmVsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgdHJhbnNsYXRlTGFiZWwgPSBmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICByZXR1cm4gbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpO1xuICAgICAgfTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogZGF0YS50aW1lc3RhbXBzW1wiQ1JFQVRFRFwiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IGRhdGEudGltZXN0YW1wc1tcIkNSRUFURURcIl0gKyAxLFxuICAgICAgICAgICAgICB0eXBlOiAnc2NoZWR1bGVkJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgIF1cbiAgICAgICAgfSk7XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnZlcnRpY2VzLCBmdW5jdGlvbih2ZXJ0ZXgpIHtcbiAgICAgICAgICBpZiAodmVydGV4WydzdGFydC10aW1lJ10gPiAtMSkge1xuICAgICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSksXG4gICAgICAgICAgICAgICAgICBjb2xvcjogXCIjZDlmMWY3XCIsXG4gICAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNjJjZGVhXCIsXG4gICAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICBsaW5rOiB2ZXJ0ZXguaWQsXG4gICAgICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIF1cbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKGZ1bmN0aW9uKGQsIGksIGRhdHVtKSB7XG4gICAgICAgICAgaWYgKGQubGluaykge1xuICAgICAgICAgICAgcmV0dXJuICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICAgICAgICAgICAgam9iaWQ6IGRhdGEuamlkLFxuICAgICAgICAgICAgICB2ZXJ0ZXhJZDogZC5saW5rXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJtYWluXCIpLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnNob3dCb3JkZXJMaW5lKCkuc2hvd0hvdXJUaW1lbGluZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLmpvYiwgZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICBpZiAoZGF0YSkge1xuICAgICAgICAgIHJldHVybiBhbmFseXplVGltZShkYXRhKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdqb2JQbGFuJywgZnVuY3Rpb24oJHRpbWVvdXQpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSdncmFwaCcgd2lkdGg9JzUwMCcgaGVpZ2h0PSc0MDAnPjxnIC8+PC9zdmc+IDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz4gPGRpdiBjbGFzcz0nYnRuLWdyb3VwIHpvb20tYnV0dG9ucyc+IDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+IDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1vdXQnIG5nLWNsaWNrPSd6b29tT3V0KCknPjxpIGNsYXNzPSdmYSBmYS1taW51cycgLz48L2E+IDwvZGl2PlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBwbGFuOiAnPSdcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGNvbnRhaW5lclcsIGNyZWF0ZUVkZ2UsIGNyZWF0ZUxhYmVsRWRnZSwgY3JlYXRlTGFiZWxOb2RlLCBjcmVhdGVOb2RlLCBkM21haW5TdmcsIGQzbWFpblN2Z0csIGQzdG1wU3ZnLCBkcmF3R3JhcGgsIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiwgZ2V0Tm9kZVR5cGUsIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUsIGpvYmlkLCBsb2FkSnNvblRvRGFncmUsIG1haW5HLCBtYWluU3ZnRWxlbWVudCwgbWFpblRtcEVsZW1lbnQsIG1haW5ab29tLCBzZWFyY2hGb3JOb2RlLCBzaG9ydGVuU3RyaW5nLCBzdWJncmFwaHM7XG4gICAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKTtcbiAgICAgIHN1YmdyYXBocyA9IFtdO1xuICAgICAgam9iaWQgPSBhdHRycy5qb2JpZDtcbiAgICAgIG1haW5TdmdFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpbkcgPSBlbGVtLmNoaWxkcmVuKCkuY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5UbXBFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzFdO1xuICAgICAgZDNtYWluU3ZnID0gZDMuc2VsZWN0KG1haW5TdmdFbGVtZW50KTtcbiAgICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpO1xuICAgICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChlbGVtLmNoaWxkcmVuKClbMF0pLndpZHRoKGNvbnRhaW5lclcpO1xuICAgICAgc2NvcGUuem9vbUluID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPCAyLjk5KSB7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSArIDAuMSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIHNjb3BlLnpvb21PdXQgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA+IDAuMzEpIHtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpIC0gMC4xKTtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxFZGdlID0gZnVuY3Rpb24oZWwpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIlwiO1xuICAgICAgICBpZiAoKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkgfHwgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9IG51bGwpKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiO1xuICAgICAgICAgIGlmIChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZWwuc2hpcF9zdHJhdGVneTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnRlbXBfbW9kZSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiIChcIiArIGVsLnRlbXBfbW9kZSArIFwiKVwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwubG9jYWxfc3RyYXRlZ3kgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiw8YnI+XCIgKyBlbC5sb2NhbF9zdHJhdGVneTtcbiAgICAgICAgICB9XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUgPSBmdW5jdGlvbihpbmZvKSB7XG4gICAgICAgIHJldHVybiBpbmZvID09PSBcInBhcnRpYWxTb2x1dGlvblwiIHx8IGluZm8gPT09IFwibmV4dFBhcnRpYWxTb2x1dGlvblwiIHx8IGluZm8gPT09IFwid29ya3NldFwiIHx8IGluZm8gPT09IFwibmV4dFdvcmtzZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uU2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvbkRlbHRhXCI7XG4gICAgICB9O1xuICAgICAgZ2V0Tm9kZVR5cGUgPSBmdW5jdGlvbihlbCwgaW5mbykge1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1taXJyb3InO1xuICAgICAgICB9IGVsc2UgaWYgKGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbykpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtaXRlcmF0aW9uJztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbm9ybWFsJztcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsTm9kZSA9IGZ1bmN0aW9uKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdGVwTmFtZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIjtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuZGVzY3JpcHRpb24gPT09IFwiXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvbjtcbiAgICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpO1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwucGFyYWxsZWxpc20gIT09IFwiXCIpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5vcGVyYXRvciAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1Pk9wZXJhdGlvbjogXCIgKyBzaG9ydGVuU3RyaW5nKGVsLm9wZXJhdG9yX3N0cmF0ZWd5KSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvYT5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gZnVuY3Rpb24oaWQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN2Z0lEO1xuICAgICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWQ7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIHNob3J0ZW5TdHJpbmcgPSBmdW5jdGlvbihzKSB7XG4gICAgICAgIHZhciBzYnI7XG4gICAgICAgIGlmIChzLmNoYXJBdCgwKSA9PT0gXCI8XCIpIHtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIik7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IFwiXCI7XG4gICAgICAgIHdoaWxlIChzLmxlbmd0aCA+IDMwKSB7XG4gICAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCI7XG4gICAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aCk7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gc2JyICsgcztcbiAgICAgICAgcmV0dXJuIHNicjtcbiAgICAgIH07XG4gICAgICBjcmVhdGVOb2RlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIGlmIChpc1BhcmVudCA9PSBudWxsKSB7XG4gICAgICAgICAgaXNQYXJlbnQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuaWQgPT09IGRhdGEucGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEud29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwid29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF93b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0V29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25TZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9kZWx0YSkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25EZWx0YVwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVFZGdlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpIHtcbiAgICAgICAgdmFyIG1pc3NpbmdOb2RlO1xuICAgICAgICBpZiAoZXhpc3RpbmdOb2Rlcy5pbmRleE9mKHByZWQuaWQpICE9PSAtMSkge1xuICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UocHJlZC5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZCk7XG4gICAgICAgICAgaWYgKCEoIW1pc3NpbmdOb2RlIHx8IG1pc3NpbmdOb2RlLmFscmVhZHlBZGRlZCA9PT0gdHJ1ZSkpIHtcbiAgICAgICAgICAgIG1pc3NpbmdOb2RlLmFscmVhZHlBZGRlZCA9IHRydWU7XG4gICAgICAgICAgICBnLnNldE5vZGUobWlzc2luZ05vZGUuaWQsIHtcbiAgICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShtaXNzaW5nTm9kZSwgXCJtaXJyb3JcIiksXG4gICAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKG1pc3NpbmdOb2RlLCAnbWlycm9yJylcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShtaXNzaW5nTm9kZS5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShtaXNzaW5nTm9kZSksXG4gICAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBsb2FkSnNvblRvRGFncmUgPSBmdW5jdGlvbihnLCBkYXRhKSB7XG4gICAgICAgIHZhciBlbCwgZXhpc3RpbmdOb2RlcywgaXNQYXJlbnQsIGssIGwsIGxlbiwgbGVuMSwgbWF4SCwgbWF4VywgcHJlZCwgciwgcmVmLCBzZywgdG9JdGVyYXRlO1xuICAgICAgICBleGlzdGluZ05vZGVzID0gW107XG4gICAgICAgIGlmIChkYXRhLm5vZGVzICE9IG51bGwpIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvbjtcbiAgICAgICAgICBpc1BhcmVudCA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgICAgZm9yIChrID0gMCwgbGVuID0gdG9JdGVyYXRlLmxlbmd0aDsgayA8IGxlbjsgaysrKSB7XG4gICAgICAgICAgZWwgPSB0b0l0ZXJhdGVba107XG4gICAgICAgICAgbWF4VyA9IDA7XG4gICAgICAgICAgbWF4SCA9IDA7XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgICBub2Rlc2VwOiAyMCxcbiAgICAgICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICAgICAgcmFua3NlcDogMjAsXG4gICAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICAgICAgbWFyZ2lueDogMTAsXG4gICAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZztcbiAgICAgICAgICAgIGxvYWRKc29uVG9EYWdyZShzZywgZWwpO1xuICAgICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZyk7XG4gICAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aDtcbiAgICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodDtcbiAgICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpO1xuICAgICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaChlbC5pZCk7XG4gICAgICAgICAgaWYgKGVsLmlucHV0cyAhPSBudWxsKSB7XG4gICAgICAgICAgICByZWYgPSBlbC5pbnB1dHM7XG4gICAgICAgICAgICBmb3IgKGwgPSAwLCBsZW4xID0gcmVmLmxlbmd0aDsgbCA8IGxlbjE7IGwrKykge1xuICAgICAgICAgICAgICBwcmVkID0gcmVmW2xdO1xuICAgICAgICAgICAgICBjcmVhdGVFZGdlKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGc7XG4gICAgICB9O1xuICAgICAgc2VhcmNoRm9yTm9kZSA9IGZ1bmN0aW9uKGRhdGEsIG5vZGVJRCkge1xuICAgICAgICB2YXIgZWwsIGksIGo7XG4gICAgICAgIGZvciAoaSBpbiBkYXRhLm5vZGVzKSB7XG4gICAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldO1xuICAgICAgICAgIGlmIChlbC5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICByZXR1cm4gZWw7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICAgIGZvciAoaiBpbiBlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGRyYXdHcmFwaCA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGcsIGksIG5ld1NjYWxlLCByZW5kZXJlciwgc2csIHhDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXQ7XG4gICAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgbm9kZXNlcDogNzAsXG4gICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICByYW5rc2VwOiA1MCxcbiAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgbWFyZ2lueDogNDAsXG4gICAgICAgICAgbWFyZ2lueTogNDBcbiAgICAgICAgfSk7XG4gICAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKTtcbiAgICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKTtcbiAgICAgICAgZm9yIChpIGluIHN1YmdyYXBocykge1xuICAgICAgICAgIHNnID0gc3ViZ3JhcGhzW2ldO1xuICAgICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZyk7XG4gICAgICAgIH1cbiAgICAgICAgbmV3U2NhbGUgPSAwLjU7XG4gICAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgeUNlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkuaGVpZ2h0KCkgLSBnLmdyYXBoKCkuaGVpZ2h0ICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKTtcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgdmFyIGV2O1xuICAgICAgICAgIGV2ID0gZDMuZXZlbnQ7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgcmV0dXJuIG1haW5ab29tKGQzbWFpblN2Zyk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnBsYW4sIGZ1bmN0aW9uKG5ld1BsYW4pIHtcbiAgICAgICAgaWYgKG5ld1BsYW4pIHtcbiAgICAgICAgICByZXR1cm4gZHJhd0dyYXBoKG5ld1BsYW4pO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnSm9ic1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSAtPlxuICBjdXJyZW50Sm9iID0gbnVsbFxuICBjdXJyZW50UGxhbiA9IG51bGxcbiAgZGVmZXJyZWRzID0ge31cbiAgam9icyA9IHtcbiAgICBydW5uaW5nOiBbXVxuICAgIGZpbmlzaGVkOiBbXVxuICAgIGNhbmNlbGxlZDogW11cbiAgICBmYWlsZWQ6IFtdXG4gIH1cblxuICBqb2JPYnNlcnZlcnMgPSBbXVxuXG4gIG5vdGlmeU9ic2VydmVycyA9IC0+XG4gICAgYW5ndWxhci5mb3JFYWNoIGpvYk9ic2VydmVycywgKGNhbGxiYWNrKSAtPlxuICAgICAgY2FsbGJhY2soKVxuXG4gIEByZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxuICAgIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKVxuXG4gIEB1blJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjaylcbiAgICBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKVxuXG4gIEBzdGF0ZUxpc3QgPSAtPlxuICAgIFsgXG4gICAgICAjICdDUkVBVEVEJ1xuICAgICAgJ1NDSEVEVUxFRCdcbiAgICAgICdERVBMT1lJTkcnXG4gICAgICAnUlVOTklORydcbiAgICAgICdGSU5JU0hFRCdcbiAgICAgICdGQUlMRUQnXG4gICAgICAnQ0FOQ0VMSU5HJ1xuICAgICAgJ0NBTkNFTEVEJ1xuICAgIF1cblxuICBAdHJhbnNsYXRlTGFiZWxTdGF0ZSA9IChzdGF0ZSkgLT5cbiAgICBzd2l0Y2ggc3RhdGUudG9Mb3dlckNhc2UoKVxuICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gJ3N1Y2Nlc3MnXG4gICAgICB3aGVuICdmYWlsZWQnIHRoZW4gJ2RhbmdlcidcbiAgICAgIHdoZW4gJ3NjaGVkdWxlZCcgdGhlbiAnZGVmYXVsdCdcbiAgICAgIHdoZW4gJ2RlcGxveWluZycgdGhlbiAnaW5mbydcbiAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gJ3ByaW1hcnknXG4gICAgICB3aGVuICdjYW5jZWxpbmcnIHRoZW4gJ3dhcm5pbmcnXG4gICAgICB3aGVuICdwZW5kaW5nJyB0aGVuICdpbmZvJ1xuICAgICAgd2hlbiAndG90YWwnIHRoZW4gJ2JsYWNrJ1xuICAgICAgZWxzZSAnZGVmYXVsdCdcblxuICBAc2V0RW5kVGltZXMgPSAobGlzdCkgLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggbGlzdCwgKGpvYiwgam9iS2V5KSAtPlxuICAgICAgdW5sZXNzIGpvYlsnZW5kLXRpbWUnXSA+IC0xXG4gICAgICAgIGpvYlsnZW5kLXRpbWUnXSA9IGpvYlsnc3RhcnQtdGltZSddICsgam9iWydkdXJhdGlvbiddXG5cbiAgQGxpc3RKb2JzID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JvdmVydmlld1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsIChsaXN0LCBsaXN0S2V5KSA9PlxuICAgICAgICBzd2l0Y2ggbGlzdEtleVxuICAgICAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gam9icy5ydW5uaW5nID0gQHNldEVuZFRpbWVzKGxpc3QpXG4gICAgICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gam9icy5maW5pc2hlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2NhbmNlbGxlZCcgdGhlbiBqb2JzLmNhbmNlbGxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiBqb2JzLmZhaWxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XG4gICAgam9ic1t0eXBlXVxuXG4gIEBnZXRBbGxKb2JzID0gLT5cbiAgICBqb2JzXG5cbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XG4gICAgY3VycmVudEpvYiA9IG51bGxcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZFxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgICMgY29uc29sZS5sb2cgZGF0YS52ZXJ0aWNlc1xuICAgICAgQHNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpXG4gICAgICAjIGNvbnNvbGUubG9nIGRhdGEudmVydGljZXNcblxuICAgICAgIyAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXNcIlxuICAgICAgIyAuc3VjY2VzcyAodmVydGljZXMpIC0+XG4gICAgICAjICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIHZlcnRpY2VzKVxuXG4gICAgICAgICMgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnNJbmZvP2dldD1qb2Imam9iPVwiICsgam9iaWRcbiAgICAgICAgIyAuc3VjY2VzcyAob2xkVmVydGljZXMpIC0+XG4gICAgICAgICMgICBkYXRhLm9sZFYgPSBvbGRWZXJ0aWNlc1swXVxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvY29uZmlnXCJcbiAgICAgIC5zdWNjZXNzIChqb2JDb25maWcpIC0+XG4gICAgICAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCBqb2JDb25maWcpXG5cbiAgICAgICAgY3VycmVudEpvYiA9IGRhdGFcblxuICAgICAgICBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZVxuXG4gIEBsb2FkUGxhbiA9IChqb2JpZCkgLT5cbiAgICBjdXJyZW50UGxhbiA9IG51bGxcbiAgICBkZWZlcnJlZHMucGxhbiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQgKyBcIi9wbGFuXCJcbiAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YVxuXG4gICAgICBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZHMucGxhbi5wcm9taXNlXG5cbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cbiAgICAgIGZvciBub2RlIGluIGRhdGFcbiAgICAgICAgcmV0dXJuIG5vZGUgaWYgbm9kZS5pZCBpcyBub2RlaWRcbiAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pIGlmIG5vZGUuc3RlcF9mdW5jdGlvblxuICAgICAgICByZXR1cm4gc3ViIGlmIHN1YlxuXG4gICAgICBudWxsXG5cblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgIyAkcS5hbGwoW2RlZmVycmVkcy5wbGFuLnByb21pc2UsIGRlZmVycmVkcy5qb2IucHJvbWlzZV0pLnRoZW4gKGRhdGEpID0+XG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuICAgICAgIyBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRQbGFuLm5vZGVzKVxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpXG5cbiAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBAc2Vla1ZlcnRleChub2RlaWQpXG5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBzZWVrVmVydGV4ID0gKG5vZGVpZCkgLT5cbiAgICBmb3IgdmVydGV4IGluIGN1cnJlbnRKb2IudmVydGljZXNcbiAgICAgIHJldHVybiB2ZXJ0ZXggaWYgdmVydGV4LmlkIGlzIG5vZGVpZFxuXG4gICAgcmV0dXJuIG51bGxcblxuICBAZ2V0VmVydGV4ID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuICAgICAgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICB2ZXJ0ZXguc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzXG5cbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGxvYWRFeGNlcHRpb25zID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRxLmFsbChbZGVmZXJyZWRzLmpvYi5wcm9taXNlXSkudGhlbiAoZGF0YSkgPT5cblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIlxuICAgICAgLnN1Y2Nlc3MgKGV4Y2VwdGlvbnMpIC0+XG4gICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnNcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9ic1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRsb2csIGFtTW9tZW50LCAkcSwgJHRpbWVvdXQpIHtcbiAgdmFyIGN1cnJlbnRKb2IsIGN1cnJlbnRQbGFuLCBkZWZlcnJlZHMsIGpvYk9ic2VydmVycywgam9icywgbm90aWZ5T2JzZXJ2ZXJzO1xuICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgY3VycmVudFBsYW4gPSBudWxsO1xuICBkZWZlcnJlZHMgPSB7fTtcbiAgam9icyA9IHtcbiAgICBydW5uaW5nOiBbXSxcbiAgICBmaW5pc2hlZDogW10sXG4gICAgY2FuY2VsbGVkOiBbXSxcbiAgICBmYWlsZWQ6IFtdXG4gIH07XG4gIGpvYk9ic2VydmVycyA9IFtdO1xuICBub3RpZnlPYnNlcnZlcnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGpvYk9ic2VydmVycywgZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICAgIHJldHVybiBjYWxsYmFjaygpO1xuICAgIH0pO1xuICB9O1xuICB0aGlzLnJlZ2lzdGVyT2JzZXJ2ZXIgPSBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjayk7XG4gIH07XG4gIHRoaXMudW5SZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICB2YXIgaW5kZXg7XG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjayk7XG4gICAgcmV0dXJuIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpO1xuICB9O1xuICB0aGlzLnN0YXRlTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBbJ1NDSEVEVUxFRCcsICdERVBMT1lJTkcnLCAnUlVOTklORycsICdGSU5JU0hFRCcsICdGQUlMRUQnLCAnQ0FOQ0VMSU5HJywgJ0NBTkNFTEVEJ107XG4gIH07XG4gIHRoaXMudHJhbnNsYXRlTGFiZWxTdGF0ZSA9IGZ1bmN0aW9uKHN0YXRlKSB7XG4gICAgc3dpdGNoIChzdGF0ZS50b0xvd2VyQ2FzZSgpKSB7XG4gICAgICBjYXNlICdmaW5pc2hlZCc6XG4gICAgICAgIHJldHVybiAnc3VjY2Vzcyc7XG4gICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICByZXR1cm4gJ2Rhbmdlcic7XG4gICAgICBjYXNlICdzY2hlZHVsZWQnOlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgICAgY2FzZSAnZGVwbG95aW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3J1bm5pbmcnOlxuICAgICAgICByZXR1cm4gJ3ByaW1hcnknO1xuICAgICAgY2FzZSAnY2FuY2VsaW5nJzpcbiAgICAgICAgcmV0dXJuICd3YXJuaW5nJztcbiAgICAgIGNhc2UgJ3BlbmRpbmcnOlxuICAgICAgICByZXR1cm4gJ2luZm8nO1xuICAgICAgY2FzZSAndG90YWwnOlxuICAgICAgICByZXR1cm4gJ2JsYWNrJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgfVxuICB9O1xuICB0aGlzLnNldEVuZFRpbWVzID0gZnVuY3Rpb24obGlzdCkge1xuICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2gobGlzdCwgZnVuY3Rpb24oam9iLCBqb2JLZXkpIHtcbiAgICAgIGlmICghKGpvYlsnZW5kLXRpbWUnXSA+IC0xKSkge1xuICAgICAgICByZXR1cm4gam9iWydlbmQtdGltZSddID0gam9iWydzdGFydC10aW1lJ10gKyBqb2JbJ2R1cmF0aW9uJ107XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JvdmVydmlld1wiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbihsaXN0LCBsaXN0S2V5KSB7XG4gICAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMucnVubmluZyA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5maW5pc2hlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnY2FuY2VsbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuY2FuY2VsbGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5mYWlsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpO1xuICAgICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JzID0gZnVuY3Rpb24odHlwZSkge1xuICAgIHJldHVybiBqb2JzW3R5cGVdO1xuICB9O1xuICB0aGlzLmdldEFsbEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gam9icztcbiAgfTtcbiAgdGhpcy5sb2FkSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIF90aGlzLnNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGpvYkNvbmZpZykge1xuICAgICAgICAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCBqb2JDb25maWcpO1xuICAgICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5sb2FkUGxhbiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgY3VycmVudFBsYW4gPSBudWxsO1xuICAgIGRlZmVycmVkcy5wbGFuID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvcGxhblwiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZHMucGxhbi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaSwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGkgPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2ldO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGZvdW5kTm9kZTtcbiAgICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpO1xuICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleChub2RlaWQpO1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuc2Vla1ZlcnRleCA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIHZhciBpLCBsZW4sIHJlZiwgdmVydGV4O1xuICAgIHJlZiA9IGN1cnJlbnRKb2IudmVydGljZXM7XG4gICAgZm9yIChpID0gMCwgbGVuID0gcmVmLmxlbmd0aDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgICB2ZXJ0ZXggPSByZWZbaV07XG4gICAgICBpZiAodmVydGV4LmlkID09PSBub2RlaWQpIHtcbiAgICAgICAgcmV0dXJuIHZlcnRleDtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH07XG4gIHRoaXMuZ2V0VmVydGV4ID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRxLmFsbChbZGVmZXJyZWRzLmpvYi5wcm9taXNlXSkudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciB2ZXJ0ZXg7XG4gICAgICAgIHZlcnRleCA9IF90aGlzLnNlZWtWZXJ0ZXgodmVydGV4aWQpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrdGltZXNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmxvYWRFeGNlcHRpb25zID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkcS5hbGwoW2RlZmVycmVkcy5qb2IucHJvbWlzZV0pLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIikuc3VjY2VzcyhmdW5jdGlvbihleGNlcHRpb25zKSB7XG4gICAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9ucztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShleGNlcHRpb25zKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnT3ZlcnZpZXdDb250cm9sbGVyJywgKCRzY29wZSwgT3ZlcnZpZXdTZXJ2aWNlLCBKb2JzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcbiAgICAkc2NvcGUuZmluaXNoZWRKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4gIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5vdmVydmlldyA9IGRhdGFcblxuICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ092ZXJ2aWV3Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgT3ZlcnZpZXdTZXJ2aWNlLCBKb2JzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLnJ1bm5pbmdKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpO1xuICAgIHJldHVybiAkc2NvcGUuZmluaXNoZWRKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gICRzY29wZS5qb2JPYnNlcnZlcigpO1xuICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5vdmVydmlldyA9IGRhdGE7XG4gIH0pO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ092ZXJ2aWV3U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBvdmVydmlldyA9IHt9XG5cbiAgQGxvYWRPdmVydmlldyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvb3ZlcnZpZXdcIilcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBvdmVydmlldyA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdPdmVydmlld1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBvdmVydmlldztcbiAgb3ZlcnZpZXcgPSB7fTtcbiAgdGhpcy5sb2FkT3ZlcnZpZXcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9vdmVydmlld1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBvdmVydmlldyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiJdLCJzb3VyY2VSb290IjoiL3NvdXJjZS8ifQ==