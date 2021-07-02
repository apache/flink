package org.apache.flink.table.client.cli;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CliStringsITCase {

    private final String single_space = " ";
    private final String empty = "";
    final String lineBreaker = "\n";

    @Test
    public void testCompletenessOfCliClientCommandsInHelp() throws IOException {
        final String operationSuffix = "OPERATION";
        final String targetPackage = "org.apache.flink.table.operations.command";
        List<String> hintsInHelpMessage = getCommandsHintsInHelpMessage();
        List<String> hintsInPackage = getCommandsHintsInPackage(targetPackage, operationSuffix);
        if (CollectionUtils.isNotEmpty(hintsInPackage)) {
            if (CollectionUtils.isNotEmpty(hintsInHelpMessage)) {
                for (String hintInPkg : hintsInPackage) {
                    boolean startWith = false;
                    for (String hintInHelpMsg : hintsInHelpMessage) {
                        if (hintInHelpMsg.startsWith(hintInPkg)) {
                            startWith = true;
                            break;
                        }
                    }
                    Assert.assertTrue(startWith);
                }
            } else {
                Assert.fail(
                        "CliStrings.HELP_MESSAGE doesn't contain commands completely in package 'org.apache.flink.table.operations.command'.");
            }
        }
    }

    /**
     * Get Commands Hints in package {@link org.apache.flink.table.operations.command}
     *
     * @return A list which contains operation name in upper case without 'Operation' suffix and space.
     *
     * @throws IOException exception.
     */
    public List<String> getCommandsHintsInPackage(String packageName, String suffix)
            throws IOException {
        ImmutableSet<ClassPath.ClassInfo> topLevelClasses =
                ClassPath.from(Thread.currentThread().getContextClassLoader())
                        .getTopLevelClasses(packageName);
        return topLevelClasses.stream()
                .map(classInfo -> classInfo.getSimpleName().toUpperCase()).map(upperClassname -> {
                    int posStartSuffix = upperClassname.lastIndexOf(suffix);
                    return posStartSuffix > -1 ? upperClassname.substring(0, posStartSuffix) : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Get Commands Hints in {@link org.apache.flink.table.client.cli.CliStrings#MESSAGE_HELP}
     *
     * @return A list which contains operation name in upper case.
     *
     * @throws IOException exception.
     */
    private List<String> getCommandsHintsInHelpMessage() {
        return Arrays
                .stream(CliStrings.MESSAGE_HELP.toString().split(lineBreaker))
                .filter(line -> StringUtils.isNotBlank(line)
                        && line.contains(CliStrings.CMD_DESC_DELIMITER))
                .map(line -> line.split(CliStrings.CMD_DESC_DELIMITER)[0]
                        .toUpperCase()
                        .replaceAll(single_space, empty))
                .collect(Collectors.toList());
    }
}







