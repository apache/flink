################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse

from org.apache.flink.api.common.functions import FlatMapFunction
from org.apache.flink.api.common.functions import ReduceFunction
from org.apache.flink.api.java.functions import KeySelector

TARGET_VAL = 100
MAX_INT_START = 50

default_input_data = [
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,",
    "And by opposing end them?--To die,--to sleep,--",
    "No more; and by a sleep to say we end",
    "The heartache, and the thousand natural shocks",
    "That flesh is heir to,--'tis a consummation",
    "Devoutly to be wish'd. To die,--to sleep;--",
    "To sleep! perchance to dream:--ay, there's the rub;",
    "For in that sleep of death what dreams may come,",
    "When we have shuffled off this mortal coil,",
    "Must give us pause: there's the respect",
    "That makes calamity of so long life;",
    "For who would bear the whips and scorns of time,",
    "The oppressor's wrong, the proud man's contumely,",
    "The pangs of despis'd love, the law's delay,",
    "The insolence of office, and the spurns",
    "That patient merit of the unworthy takes,",
    "When he himself might his quietus make",
    "With a bare bodkin? who would these fardels bear,",
    "To grunt and sweat under a weary life,",
    "But that the dread of something after death,--",
    "The undiscover'd country, from whose bourn",
    "No traveller returns,--puzzles the will,",
    "And makes us rather bear those ills we have",
    "Than fly to others that we know not of?",
    "Thus conscience does make cowards of us all;",
    "And thus the native hue of resolution",
    "Is sicklied o'er with the pale cast of thought;",
    "And enterprises of great pith and moment,",
    "With this regard, their currents turn awry,",
    "And lose the name of action.--Soft you now!",
    "The fair Ophelia!--Nymph, in thy orisons",
    "Be all my sins remember'd."
]


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        for word in value.lower().split():
            collector.collect((1, word))


class Sum(ReduceFunction):
    def reduce(self, value1, value2):
        return (value1[0] + value2[0], value1[1])       


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


class Main:
    def run(self, flink, args):
        env = flink.get_execution_environment()

        if args.input:
            text = env.read_text_file(args.input)
        else:
            text = env.from_collection(default_input_data)

        counts = text \
            .flat_map(Tokenizer())\
            .key_by(Selector())\
            .reduce(Sum())

        if args.output:
            counts.write_as_text(args.output)
        else:
            counts.output()
        env.execute("Wordcount Example (py)")

"""
Implements the "WordCount" program that computes a simple word occurrence
histogram over text files in a streaming fashion.

The input is a plain text file with lines separated by newline characters.

Usage: WordCount --input <path> --output <path>
If no parameters are provided, the program is run with default data.

This example shows how to:
  * write a simple Flink Streaming program,
  * use tuple data types,
  * write and use user-defined functions.
"""
def main(flink):
    parser = argparse.ArgumentParser(description='WordCount.')
    parser.add_argument('--input', metavar='IN', help='input file path')
    parser.add_argument('--output', metavar='OUT', help='output file path')
    args = parser.parse_args()
    Main().run(flink, args)
