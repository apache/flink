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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A simple command line parser (based on Apache Commons CLI) that extracts command line options.
 */
public class CliFrontendParser {

    static final Option HELP_OPTION =
            new Option(
                    "h",
                    "help",
                    false,
                    "Show the help message for the CLI Frontend or the action.");

    static final Option JAR_OPTION = new Option("j", "jarfile", true, "Flink program JAR file.");

    static final Option CLASS_OPTION =
            new Option(
                    "c",
                    "class",
                    true,
                    "Class with the program entry point (\"main()\" method). Only needed if the "
                            + "JAR file does not specify the class in its manifest.");

    static final Option CLASSPATH_OPTION =
            new Option(
                    "C",
                    "classpath",
                    true,
                    "Adds a URL to each user code "
                            + "classloader  on all nodes in the cluster. The paths must specify a protocol (e.g. file://) and be "
                            + "accessible on all nodes (e.g. by means of a NFS share). You can use this option multiple "
                            + "times for specifying more than one URL. The protocol must be supported by the "
                            + "{@link java.net.URLClassLoader}.");

    public static final Option PARALLELISM_OPTION =
            new Option(
                    "p",
                    "parallelism",
                    true,
                    "The parallelism with which to run the program. Optional flag to override the default value "
                            + "specified in the configuration.");
    public static final Option DETACHED_OPTION =
            new Option("d", "detached", false, "If present, runs " + "the job in detached mode");

    public static final Option SHUTDOWN_IF_ATTACHED_OPTION =
            new Option(
                    "sae",
                    "shutdownOnAttachedExit",
                    false,
                    "If the job is submitted in attached mode, perform a best-effort cluster shutdown "
                            + "when the CLI is terminated abruptly, e.g., in response to a user interrupt, such as typing Ctrl + C.");

    /**
     * @deprecated use non-prefixed variant {@link #DETACHED_OPTION} for both YARN and non-YARN
     *     deployments
     */
    @Deprecated
    public static final Option YARN_DETACHED_OPTION =
            new Option(
                    "yd",
                    "yarndetached",
                    false,
                    "If present, runs "
                            + "the job in detached mode (deprecated; use non-YARN specific option instead)");

    public static final Option ARGS_OPTION =
            new Option(
                    "a",
                    "arguments",
                    true,
                    "Program arguments. Arguments can also be added without -a, simply as trailing parameters.");

    public static final Option ADDRESS_OPTION =
            new Option(
                    "m",
                    "jobmanager",
                    true,
                    "Address of the JobManager to which to connect. "
                            + "Use this flag to connect to a different JobManager than the one specified in the configuration.");

    public static final Option SAVEPOINT_PATH_OPTION =
            new Option(
                    "s",
                    "fromSavepoint",
                    true,
                    "Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537).");

    public static final Option SAVEPOINT_ALLOW_NON_RESTORED_OPTION =
            new Option(
                    "n",
                    "allowNonRestoredState",
                    false,
                    "Allow to skip savepoint state that cannot be restored. "
                            + "You need to allow this if you removed an operator from your "
                            + "program that was part of the program when the savepoint was triggered.");

    public static final Option SAVEPOINT_RESTORE_MODE =
            new Option(
                    "rm",
                    "restoreMode",
                    true,
                    "Defines how should we restore from the given savepoint. Supported options: "
                            + "[claim - claim ownership of the savepoint and delete once it is"
                            + " subsumed, no_claim (default) - do not claim ownership, the first"
                            + " checkpoint will not reuse any files from the restored one, legacy "
                            + "- the old behaviour, do not assume ownership of the savepoint files,"
                            + " but can reuse some shared files.");

    static final Option SAVEPOINT_DISPOSE_OPTION =
            new Option("d", "dispose", true, "Path of savepoint to dispose.");

    static final Option SAVEPOINT_FORMAT_OPTION =
            new Option(
                    "type",
                    "type",
                    true,
                    "Describes the binary format in which a savepoint should be taken. Supported"
                            + " options: [canonical - a common format for all state backends, allow"
                            + " for changing state backends, native = a specific format for the"
                            + " chosen state backend, might be faster to take and restore from.");

    // list specific options
    static final Option RUNNING_OPTION =
            new Option("r", "running", false, "Show only running programs and their JobIDs");

    static final Option SCHEDULED_OPTION =
            new Option("s", "scheduled", false, "Show only scheduled programs and their JobIDs");

    static final Option ALL_OPTION =
            new Option("a", "all", false, "Show all programs and their JobIDs");

    static final Option ZOOKEEPER_NAMESPACE_OPTION =
            new Option(
                    "z",
                    "zookeeperNamespace",
                    true,
                    "Namespace to create the Zookeeper sub-paths for high availability mode");

    static final Option CANCEL_WITH_SAVEPOINT_OPTION =
            new Option(
                    "s",
                    "withSavepoint",
                    true,
                    "**DEPRECATION WARNING**: "
                            + "Cancelling a job with savepoint is deprecated. Use \"stop\" instead. \n Trigger"
                            + " savepoint and cancel job. The target directory is optional. If no directory is "
                            + "specified, the configured default directory ("
                            + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                            + ") is used.");

    public static final Option STOP_WITH_SAVEPOINT_PATH =
            new Option(
                    "p",
                    "savepointPath",
                    true,
                    "Path to the savepoint (for example hdfs:///flink/savepoint-1537). "
                            + "If no directory is specified, the configured default will be used (\""
                            + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                            + "\").");

    public static final Option STOP_AND_DRAIN =
            new Option(
                    "d",
                    "drain",
                    false,
                    "Send MAX_WATERMARK before taking the savepoint and stopping the pipelne.");

    public static final Option PY_OPTION =
            new Option(
                    "py",
                    "python",
                    true,
                    "Python script with the program entry point. "
                            + "The dependent resources can be configured with the `--pyFiles` option.");

    public static final Option PYFILES_OPTION =
            new Option(
                    "pyfs",
                    "pyFiles",
                    true,
                    "Attach custom files for job. The standard resource file suffixes such as .py/.egg/.zip/.whl or directory are all supported. "
                            + "These files will be added to the PYTHONPATH of both the local client and the remote python UDF worker. "
                            + "Files suffixed with .zip will be extracted and added to PYTHONPATH. "
                            + "Comma (',') could be used as the separator to specify multiple files "
                            + "(e.g., --pyFiles file:///tmp/myresource.zip,hdfs:///$namenode_address/myresource2.zip).");

    public static final Option PYMODULE_OPTION =
            new Option(
                    "pym",
                    "pyModule",
                    true,
                    "Python module with the program entry point. "
                            + "This option must be used in conjunction with `--pyFiles`.");

    public static final Option PYREQUIREMENTS_OPTION =
            new Option(
                    "pyreq",
                    "pyRequirements",
                    true,
                    "Specify a requirements.txt file which defines the third-party dependencies. "
                            + "These dependencies will be installed and added to the PYTHONPATH of the python UDF worker. "
                            + "A directory which contains the installation packages of these dependencies could be specified "
                            + "optionally. Use '#' as the separator if the optional parameter exists "
                            + "(e.g., --pyRequirements file:///tmp/requirements.txt#file:///tmp/cached_dir).");

    public static final Option PYARCHIVE_OPTION =
            new Option(
                    "pyarch",
                    "pyArchives",
                    true,
                    "Add python archive files for job. The archive files will be extracted to the working directory "
                            + "of python UDF worker. For each archive file, a target directory "
                            + "be specified. If the target directory name is specified, the archive file will be extracted to a "
                            + "directory with the specified name. Otherwise, the archive file will be extracted to a "
                            + "directory with the same name of the archive file. The files uploaded via this option are accessible "
                            + "via relative path. '#' could be used as the separator of the archive file path and the target directory "
                            + "name. Comma (',') could be used as the separator to specify multiple archive files. "
                            + "This option can be used to upload the virtual environment, the data files used in Python UDF "
                            + "(e.g., --pyArchives file:///tmp/py37.zip,file:///tmp/data.zip#data --pyExecutable "
                            + "py37.zip/py37/bin/python). The data files could be accessed in Python UDF, e.g.: "
                            + "f = open('data/data.txt', 'r').");

    public static final Option PYEXEC_OPTION =
            new Option(
                    "pyexec",
                    "pyExecutable",
                    true,
                    "Specify the path of the python interpreter used to execute the python UDF worker "
                            + "(e.g.: --pyExecutable /usr/local/bin/python3). "
                            + "The python UDF worker depends on Python 3.8+, Apache Beam (version == 2.43.0), "
                            + "Pip (version >= 20.3) and SetupTools (version >= 37.0.0). "
                            + "Please ensure that the specified environment meets the above requirements.");

    public static final Option PYCLIENTEXEC_OPTION =
            new Option(
                    "pyclientexec",
                    "pyClientExecutable",
                    true,
                    "The path of the Python interpreter used to launch the Python "
                            + "process when submitting the Python jobs via \"flink run\" or compiling "
                            + "the Java/Scala jobs containing Python UDFs.");

    public static final Option PYTHON_PATH =
            new Option(
                    "pypath",
                    "pyPythonPath",
                    true,
                    "Specify the path of the python installation in worker nodes."
                            + "(e.g.: --pyPythonPath /python/lib64/python3.7/)."
                            + "User can specify multiple paths using the separator \":\".");

    static {
        HELP_OPTION.setRequired(false);

        JAR_OPTION.setRequired(false);
        JAR_OPTION.setArgName("jarfile");

        CLASS_OPTION.setRequired(false);
        CLASS_OPTION.setArgName("classname");

        CLASSPATH_OPTION.setRequired(false);
        CLASSPATH_OPTION.setArgName("url");

        ADDRESS_OPTION.setRequired(false);
        ADDRESS_OPTION.setArgName("host:port");

        PARALLELISM_OPTION.setRequired(false);
        PARALLELISM_OPTION.setArgName("parallelism");

        DETACHED_OPTION.setRequired(false);
        SHUTDOWN_IF_ATTACHED_OPTION.setRequired(false);
        YARN_DETACHED_OPTION.setRequired(false);

        ARGS_OPTION.setRequired(false);
        ARGS_OPTION.setArgName("programArgs");
        ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);

        RUNNING_OPTION.setRequired(false);
        SCHEDULED_OPTION.setRequired(false);

        SAVEPOINT_PATH_OPTION.setRequired(false);
        SAVEPOINT_PATH_OPTION.setArgName("savepointPath");

        SAVEPOINT_ALLOW_NON_RESTORED_OPTION.setRequired(false);
        SAVEPOINT_RESTORE_MODE.setRequired(false);

        SAVEPOINT_FORMAT_OPTION.setRequired(false);

        ZOOKEEPER_NAMESPACE_OPTION.setRequired(false);
        ZOOKEEPER_NAMESPACE_OPTION.setArgName("zookeeperNamespace");

        CANCEL_WITH_SAVEPOINT_OPTION.setRequired(false);
        CANCEL_WITH_SAVEPOINT_OPTION.setArgName("targetDirectory");
        CANCEL_WITH_SAVEPOINT_OPTION.setOptionalArg(true);

        STOP_WITH_SAVEPOINT_PATH.setRequired(false);
        STOP_WITH_SAVEPOINT_PATH.setArgName("savepointPath");
        STOP_WITH_SAVEPOINT_PATH.setOptionalArg(true);

        STOP_AND_DRAIN.setRequired(false);

        PY_OPTION.setRequired(false);
        PY_OPTION.setArgName("pythonFile");

        PYFILES_OPTION.setRequired(false);
        PYFILES_OPTION.setArgName("pythonFiles");

        PYMODULE_OPTION.setRequired(false);
        PYMODULE_OPTION.setArgName("pythonModule");

        PYREQUIREMENTS_OPTION.setRequired(false);

        PYARCHIVE_OPTION.setRequired(false);

        PYEXEC_OPTION.setRequired(false);

        PYCLIENTEXEC_OPTION.setRequired(false);

        PYTHON_PATH.setRequired(false);
    }

    static final Options RUN_OPTIONS = getRunCommandOptions();

    private static Options buildGeneralOptions(Options options) {
        options.addOption(HELP_OPTION);
        // backwards compatibility: ignore verbose flag (-v)
        options.addOption(new Option("v", "verbose", false, "This option is deprecated."));
        return options;
    }

    private static Options getProgramSpecificOptions(Options options) {
        options.addOption(JAR_OPTION);
        options.addOption(CLASS_OPTION);
        options.addOption(CLASSPATH_OPTION);
        options.addOption(PARALLELISM_OPTION);
        options.addOption(ARGS_OPTION);
        options.addOption(DETACHED_OPTION);
        options.addOption(SHUTDOWN_IF_ATTACHED_OPTION);
        options.addOption(YARN_DETACHED_OPTION);
        options.addOption(PY_OPTION);
        options.addOption(PYFILES_OPTION);
        options.addOption(PYMODULE_OPTION);
        options.addOption(PYREQUIREMENTS_OPTION);
        options.addOption(PYARCHIVE_OPTION);
        options.addOption(PYEXEC_OPTION);
        options.addOption(PYCLIENTEXEC_OPTION);
        options.addOption(PYTHON_PATH);
        return options;
    }

    private static Options getProgramSpecificOptionsWithoutDeprecatedOptions(Options options) {
        options.addOption(CLASS_OPTION);
        options.addOption(CLASSPATH_OPTION);
        options.addOption(PARALLELISM_OPTION);
        options.addOption(DETACHED_OPTION);
        options.addOption(SHUTDOWN_IF_ATTACHED_OPTION);
        options.addOption(PY_OPTION);
        options.addOption(PYFILES_OPTION);
        options.addOption(PYMODULE_OPTION);
        options.addOption(PYREQUIREMENTS_OPTION);
        options.addOption(PYARCHIVE_OPTION);
        options.addOption(PYEXEC_OPTION);
        options.addOption(PYCLIENTEXEC_OPTION);
        options.addOption(PYTHON_PATH);
        return options;
    }

    public static Options getRunCommandOptions() {
        return getProgramSpecificOptions(buildGeneralOptions(new Options()))
                .addOption(SAVEPOINT_PATH_OPTION)
                .addOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION)
                .addOption(SAVEPOINT_RESTORE_MODE);
    }

    static Options getInfoCommandOptions() {
        Options options = buildGeneralOptions(new Options());
        return getProgramSpecificOptions(options);
    }

    static Options getListCommandOptions() {
        Options options = buildGeneralOptions(new Options());
        options.addOption(ALL_OPTION);
        options.addOption(RUNNING_OPTION);
        return options.addOption(SCHEDULED_OPTION);
    }

    static Options getCancelCommandOptions() {
        return buildGeneralOptions(new Options())
                .addOption(CANCEL_WITH_SAVEPOINT_OPTION)
                .addOption(SAVEPOINT_FORMAT_OPTION);
    }

    static Options getStopCommandOptions() {
        return buildGeneralOptions(new Options())
                .addOption(STOP_WITH_SAVEPOINT_PATH)
                .addOption(STOP_AND_DRAIN)
                .addOption(SAVEPOINT_FORMAT_OPTION);
    }

    static Options getSavepointCommandOptions() {
        return buildGeneralOptions(new Options())
                .addOption(SAVEPOINT_DISPOSE_OPTION)
                .addOption(JAR_OPTION)
                .addOption(SAVEPOINT_FORMAT_OPTION);
    }

    // --------------------------------------------------------------------------------------------
    //  Help
    // --------------------------------------------------------------------------------------------

    private static Options getRunOptionsWithoutDeprecatedOptions(Options options) {
        return getProgramSpecificOptionsWithoutDeprecatedOptions(options)
                .addOption(SAVEPOINT_PATH_OPTION)
                .addOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION)
                .addOption(SAVEPOINT_RESTORE_MODE);
    }

    private static Options getInfoOptionsWithoutDeprecatedOptions(Options options) {
        options.addOption(CLASS_OPTION);
        options.addOption(PARALLELISM_OPTION);
        return options;
    }

    private static Options getListOptionsWithoutDeprecatedOptions(Options options) {
        options.addOption(RUNNING_OPTION);
        options.addOption(ALL_OPTION);
        options.addOption(SCHEDULED_OPTION);
        return options;
    }

    private static Options getCancelOptionsWithoutDeprecatedOptions(Options options) {
        return options.addOption(CANCEL_WITH_SAVEPOINT_OPTION);
    }

    private static Options getStopOptionsWithoutDeprecatedOptions(Options options) {
        return options.addOption(STOP_WITH_SAVEPOINT_PATH)
                .addOption(STOP_AND_DRAIN)
                .addOption(SAVEPOINT_FORMAT_OPTION);
    }

    private static Options getSavepointOptionsWithoutDeprecatedOptions(Options options) {
        return options.addOption(SAVEPOINT_DISPOSE_OPTION)
                .addOption(SAVEPOINT_FORMAT_OPTION)
                .addOption(JAR_OPTION);
    }

    /** Prints the help for the client. */
    public static void printHelp(Collection<CustomCommandLine> customCommandLines) {
        System.out.println("./flink <ACTION> [OPTIONS] [ARGUMENTS]");
        System.out.println();
        System.out.println("The following actions are available:");

        printHelpForRun(customCommandLines);
        printHelpForRunApplication(customCommandLines);
        printHelpForInfo();
        printHelpForList(customCommandLines);
        printHelpForStop(customCommandLines);
        printHelpForCancel(customCommandLines);
        printHelpForSavepoint(customCommandLines);

        System.out.println();
    }

    public static void printHelpForRun(Collection<CustomCommandLine> customCommandLines) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println("\nAction \"run\" compiles and runs a program.");
        System.out.println("\n  Syntax: run [OPTIONS] <jar-file> <arguments>");
        formatter.setSyntaxPrefix("  \"run\" action options:");
        formatter.printHelp(" ", getRunOptionsWithoutDeprecatedOptions(new Options()));

        printCustomCliOptions(customCommandLines, formatter, true);

        System.out.println();
    }

    public static void printHelpForRunApplication(
            Collection<CustomCommandLine> customCommandLines) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println("\nAction \"run-application\" runs an application in Application Mode.");
        System.out.println("\n  Syntax: run-application [OPTIONS] <jar-file> <arguments>");
        formatter.setSyntaxPrefix("  \"run-application\" action options:");

        // Only GenericCLI works with application mode, the other CLIs will be phased out
        // in the future
        List<CustomCommandLine> filteredCommandLines =
                customCommandLines.stream()
                        .filter((cli) -> cli instanceof GenericCLI)
                        .collect(Collectors.toList());

        printCustomCliOptions(filteredCommandLines, formatter, true);
        System.out.println();
    }

    public static void printHelpForInfo() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println(
                "\nAction \"info\" shows the optimized execution plan of the program (JSON).");
        System.out.println("\n  Syntax: info [OPTIONS] <jar-file> <arguments>");
        formatter.setSyntaxPrefix("  \"info\" action options:");
        formatter.printHelp(" ", getInfoOptionsWithoutDeprecatedOptions(new Options()));

        System.out.println();
    }

    public static void printHelpForList(Collection<CustomCommandLine> customCommandLines) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println("\nAction \"list\" lists running and scheduled programs.");
        System.out.println("\n  Syntax: list [OPTIONS]");
        formatter.setSyntaxPrefix("  \"list\" action options:");
        formatter.printHelp(" ", getListOptionsWithoutDeprecatedOptions(new Options()));

        printCustomCliOptions(customCommandLines, formatter, false);

        System.out.println();
    }

    public static void printHelpForStop(Collection<CustomCommandLine> customCommandLines) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println(
                "\nAction \"stop\" stops a running program with a savepoint (streaming jobs only).");
        System.out.println("\n  Syntax: stop [OPTIONS] <Job ID>");
        formatter.setSyntaxPrefix("  \"stop\" action options:");
        formatter.printHelp(" ", getStopOptionsWithoutDeprecatedOptions(new Options()));

        printCustomCliOptions(customCommandLines, formatter, false);

        System.out.println();
    }

    public static void printHelpForCancel(Collection<CustomCommandLine> customCommandLines) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println("\nAction \"cancel\" cancels a running program.");
        System.out.println("\n  Syntax: cancel [OPTIONS] <Job ID>");
        formatter.setSyntaxPrefix("  \"cancel\" action options:");
        formatter.printHelp(" ", getCancelOptionsWithoutDeprecatedOptions(new Options()));

        printCustomCliOptions(customCommandLines, formatter, false);

        System.out.println();
    }

    public static void printHelpForSavepoint(Collection<CustomCommandLine> customCommandLines) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println(
                "\nAction \"savepoint\" triggers savepoints for a running job or disposes existing ones.");
        System.out.println("\n  Syntax: savepoint [OPTIONS] <Job ID> [<target directory>]");
        formatter.setSyntaxPrefix("  \"savepoint\" action options:");
        formatter.printHelp(" ", getSavepointOptionsWithoutDeprecatedOptions(new Options()));

        printCustomCliOptions(customCommandLines, formatter, false);

        System.out.println();
    }

    /**
     * Prints custom cli options.
     *
     * @param formatter The formatter to use for printing
     * @param runOptions True if the run options should be printed, False to print only general
     *     options
     */
    private static void printCustomCliOptions(
            Collection<CustomCommandLine> customCommandLines,
            HelpFormatter formatter,
            boolean runOptions) {
        // prints options from all available command-line classes
        for (CustomCommandLine cli : customCommandLines) {
            formatter.setSyntaxPrefix("  Options for " + cli.getId() + " mode:");
            Options customOpts = new Options();
            cli.addGeneralOptions(customOpts);
            if (runOptions) {
                cli.addRunOptions(customOpts);
            }
            formatter.printHelp(" ", customOpts);
            System.out.println();
        }
    }

    public static SavepointRestoreSettings createSavepointRestoreSettings(CommandLine commandLine) {
        if (commandLine.hasOption(SAVEPOINT_PATH_OPTION.getOpt())) {
            String savepointPath = commandLine.getOptionValue(SAVEPOINT_PATH_OPTION.getOpt());
            boolean allowNonRestoredState =
                    commandLine.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION.getOpt());
            final RestoreMode restoreMode;
            if (commandLine.hasOption(SAVEPOINT_RESTORE_MODE)) {
                restoreMode =
                        ConfigurationUtils.convertValue(
                                commandLine.getOptionValue(SAVEPOINT_RESTORE_MODE),
                                RestoreMode.class);
            } else {
                restoreMode = SavepointConfigOptions.RESTORE_MODE.defaultValue();
            }
            return SavepointRestoreSettings.forPath(
                    savepointPath, allowNonRestoredState, restoreMode);
        } else {
            return SavepointRestoreSettings.none();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static CommandLine parse(Options options, String[] args, boolean stopAtNonOptions)
            throws CliArgsException {
        final DefaultParser parser = new DefaultParser();

        try {
            return parser.parse(options, args, stopAtNonOptions);
        } catch (ParseException e) {
            throw new CliArgsException(e.getMessage());
        }
    }

    /**
     * Merges the given {@link Options} into a new Options object.
     *
     * @param optionsA options to merge, can be null if none
     * @param optionsB options to merge, can be null if none
     * @return
     */
    public static Options mergeOptions(@Nullable Options optionsA, @Nullable Options optionsB) {
        final Options resultOptions = new Options();
        if (optionsA != null) {
            for (Option option : optionsA.getOptions()) {
                resultOptions.addOption(option);
            }
        }

        if (optionsB != null) {
            for (Option option : optionsB.getOptions()) {
                resultOptions.addOption(option);
            }
        }

        return resultOptions;
    }
}
