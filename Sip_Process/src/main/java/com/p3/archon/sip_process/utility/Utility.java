package com.p3.archon.sip_process.utility;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 4:46 PM.
 */
public class Utility {

    public static String readLineByLine(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    }

    public static String getFileName(String location, String fileNamePrefix, int fileCounter, String extension) {
        return location + File.separator + fileNamePrefix + (fileCounter > 9 ? fileCounter : "0" + fileCounter) + "." + extension;
    }

    public static void fileCreatorWithContent(PrintWriter printWriter, String content) {
        printWriter.write(content);
        endWriter(printWriter);
    }

    public static void endWriter(PrintWriter recordFileWriter) {
        recordFileWriter.flush();
        recordFileWriter.close();
    }

    public static List<String> getSetToList(Set<String> set) {
        List<String> setToList = (Arrays.asList(set.toArray(new String[set.size()])));
        Collections.sort(setToList);
        return setToList;
    }

    public static String getTextFormatted(String string) {
        string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
        string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
                ? string.substring(1).substring(0, string.length() - 2)
                : string);
        return (string.length() > 0)
                ? (((string.charAt(0) >= '0' && string.charAt(0) <= '9') ? "_" : "") + string).toUpperCase()
                : string;
    }
}
