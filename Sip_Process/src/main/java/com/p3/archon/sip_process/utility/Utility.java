package com.p3.archon.sip_process.utility;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static com.p3.archon.sip_process.constants.SipPerformanceConstant.NEW_LINE;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 4:46 PM.
 */
public class Utility {

    public static String readLineByLine(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> fileLines = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
            fileLines.forEach(line -> contentBuilder.append(line).append(NEW_LINE));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    }

    public static Long readLineByLineCount(String filePath) {
        long count = 0;
        String currentLine;
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(filePath));
            while ((currentLine = bufferedReader.readLine()) != null) {
                count++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null)
                    bufferedReader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return count;
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

    public static String getColumnAsQueryFramer(List<String> mainTablePrimaryKeyColumns) {
        List<String> COLUMN_STRING = new ArrayList<>();
        for (String column : mainTablePrimaryKeyColumns) {
            COLUMN_STRING.add(column + " as \"" + column + "\"");
        }
        return String.join(",", COLUMN_STRING).isEmpty() ? " * " : String.join(",", COLUMN_STRING);
    }

    public static String LoggerPrintFor10000Line(long count) {
        return "\n" + "======================================================" + "\n" + "" + "\n" + count + " Processed" + "\n" + "" + "\n" + "======================================================";
    }

}
