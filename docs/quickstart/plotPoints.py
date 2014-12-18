#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import matplotlib.pyplot as plt
import csv
import os

if len(sys.argv) < 4 or not sys.argv[1] in ['points', 'result']:
  print "Usage: plot-clusters.py (points|result) <src-file> <pdf-file-prefix>"
  sys.exit(1)

inFile = sys.argv[1]
inFile = sys.argv[2]
outFilePx = sys.argv[3]

inFileName = os.path.splitext(os.path.basename(inFile))[0]
outFile = os.path.join(".", outFilePx+"-plot.pdf")

########### READ DATA

cs = []
xs = []
ys = []

minX = None
maxX = None
minY = None
maxY = None

if sys.argv[1] == 'points':

  with open(inFile, 'rb') as file:
    for line in file:
      # parse data
      csvData = line.strip().split(' ')

      x = float(csvData[0])
      y = float(csvData[1])

      if not minX or minX > x:
        minX = x
      if not maxX or maxX < x:
        maxX = x
      if not minY or minY > y:
        minY = y
      if not maxY or maxY < y:
        maxY = y

      xs.append(x)
      ys.append(y)

    # plot data
    plt.clf()
    plt.scatter(xs, ys, s=25, c="#999999", edgecolors='None', alpha=1.0)
    plt.ylim([minY,maxY])
    plt.xlim([minX,maxX])

elif sys.argv[1] == 'result':

  with open(inFile, 'rb') as file:
    for line in file:
      # parse data
      csvData = line.strip().split(' ')

      c = int(csvData[0])
      x = float(csvData[1])
      y = float(csvData[2])

      cs.append(c)
      xs.append(x)
      ys.append(y)

    # plot data
    plt.clf()
    plt.scatter(xs, ys, s=25, c=cs, edgecolors='None', alpha=1.0)
    plt.ylim([minY,maxY])
    plt.xlim([minX,maxX])


plt.savefig(outFile, dpi=600)
print "\nPlotted file: %s" % outFile

sys.exit(0)