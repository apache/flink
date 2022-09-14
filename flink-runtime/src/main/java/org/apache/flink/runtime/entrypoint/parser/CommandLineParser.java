/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint.parser;

import org.apache.flink.runtime.entrypoint.FlinkParseException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nonnull;

/** Command line parser which produces a result from the given command line arguments. */
public class CommandLineParser<T> {

    @Nonnull private final ParserResultFactory<T> parserResultFactory;

    public CommandLineParser(@Nonnull ParserResultFactory<T> parserResultFactory) {
        this.parserResultFactory = parserResultFactory;
    }

    public T parse(@Nonnull String[] args) throws FlinkParseException {
        final DefaultParser parser = new DefaultParser();
        final Options options = parserResultFactory.getOptions();

        final CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args, true);
        } catch (ParseException e) {
            throw new FlinkParseException("Failed to parse the command line arguments.", e);
        }

        return parserResultFactory.createResult(commandLine);
    }

    public void printHelp(@Nonnull String cmdLineSyntax) {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setLeftPadding(5);
        helpFormatter.setWidth(80);
        helpFormatter.printHelp(cmdLineSyntax, parserResultFactory.getOptions(), true);
    }
}
