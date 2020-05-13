package com.p3.archon.sip_process.parser;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvValidationException;
import com.p3.archon.sip_process.utility.Utility;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Suriyanarayanan K
 * on 23/04/20 12:12 PM.
 */
public class SipIntermediateJsonParser {

    private List<String> headerList = null;
    private String fileLocationWithName = null;
    private Map<String, Long> tableColumnCount = null;
    private Map<String, List<String>> tableColumnValues = new LinkedHashMap<>();
    private List<String> tablesList = new ArrayList<>();
    private String mainTable;

    private Map<String, List<Integer>> tablePrimaryHeaderPosition;
    private int fileCounter;
    private String outputLocation;
    private int mainTablePrimaryKeyCount;

    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public SipIntermediateJsonParser(String fileLocationWithName, Map<String, Long> tableColumnCount, List<String> headerList,
                                     Map<String, List<Integer>> tablePrimaryHeaderPosition, int fileCounter,
                                     String outputLocation, int mainTablePrimaryKeyCount) {
        this.headerList = headerList;
        this.fileLocationWithName = fileLocationWithName;
        this.tableColumnCount = tableColumnCount;
        this.tablesList.addAll(tableColumnCount.keySet());
        this.mainTable = tablesList.get(0);
        this.tablePrimaryHeaderPosition = tablePrimaryHeaderPosition;
        this.fileCounter = fileCounter;
        this.outputLocation = outputLocation;
        this.mainTablePrimaryKeyCount = mainTablePrimaryKeyCount;
    }


    public long startParsing(String mainTableRowRecordValue, long lineSkipper) {
        JSONObject rootTableJson = new JSONObject();
        long nextLineSkipper = parseAllLinesAndPrepareIntermediateJson(rootTableJson, mainTableRowRecordValue, lineSkipper);
        PrintWriter intermediateJsonWriter = null;
        try {
            intermediateJsonWriter = new PrintWriter(Utility.getFileName(outputLocation, "IntermediateJson_", fileCounter, "json"));
        } catch (FileNotFoundException e) {
            LOGGER.error("IntermediateJson File Not Found : " + e.getMessage());
        }
        Utility.fileCreatorWithContent(intermediateJsonWriter, rootTableJson.toString());
        return nextLineSkipper;
    }

    private long parseAllLinesAndPrepareIntermediateJson(JSONObject rootTableJson, String mainTableRowRecordValue, long lineSkipper) {
        CSVParser parser;
        int counter = 0;
        try {
            parser = new CSVParserBuilder().withSeparator('|')
                    .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH).withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreQuotations(false).withQuoteChar('"').withStrictQuotes(false).build();
            CSVReader csvReader = new CSVReaderBuilder(new FileReader(fileLocationWithName))
                    .withCSVParser(parser)
                    .withSkipLines(1).build();
            String[] line;
            rootTableJson.put(tablesList.get(0), new JSONObject());

            while ((line = csvReader.readNext()) != null) {
                if (lineSkipper != 0 && counter <= lineSkipper) {
                    if (counter != lineSkipper) {
                        counter++;
                        continue;
                    }
                }
                if (checkMatching(line, mainTableRowRecordValue)) {
                    parseJsonResult(rootTableJson.getJSONObject(tablesList.get(0)), line, tableColumnCount, tablesList, tablesList.get(0), new ArrayList<>(), 0, 0, 0);
                } else {

                    return counter++;
                }
                counter++;
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
        return counter;
    }

    private boolean checkMatching(String[] line, String mainTableRowRecordValue) {
        List<String> matchingCheckingList = new ArrayList<>();
        for (int i = 0; i < mainTablePrimaryKeyCount; i++) {
            matchingCheckingList.add(line[i]);
        }
        return (mainTableRowRecordValue.equalsIgnoreCase(String.join(",", matchingCheckingList)));
    }

    private JSONObject parseJsonResult(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, int uniqueValue) {
        JSONObject columnValuePair = new JSONObject();
        if (!checkList.contains(tableName)) {
            checkList.add(tableName);
            String idValue = getUniqueValue(line, tablePrimaryHeaderPosition.get(tableName));
            if (!idValue.contains("null")) {
                if (result.isEmpty()) {
                    parseLineaAndInsertIntoJson(result, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, columnValuePair, idValue);
                } else {
                    parseLineInsertorUpdateJson(result, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, columnValuePair, idValue);
                }
            }
        }
        return result;
    }

    private void parseLineInsertorUpdateJson(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, JSONObject columnValuePair, String idValue) {
        Set<String> rowKeyValueSet = result.keySet();
        if (rowKeyValueSet.contains(idValue)) {
            traverseJsonIntoNextLevel(result, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, idValue);
        } else {
            updateJsonAndTraverseIntoNextLevel(result, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, columnValuePair, idValue);
        }
    }

    private void updateJsonAndTraverseIntoNextLevel(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, JSONObject columnValuePair, String idValue) {
        result.put(idValue, new JSONObject());
        createColumnValuePair(line, tableColumnValues, tableName, lineStartPositon, columnValuePair);
        tablePosition = tablePosition + 1;
        if (tablePosition >= tableList.size()) {
            tablePosition = tableList.size();
        } else {
            columnValuePair.put(tableList.get(tablePosition), new JSONObject());
            columnValuePair.put(tableList.get(tablePosition), parseJsonResult(columnValuePair.getJSONObject(tableList.get(tablePosition)), line, tableColumnValues, tableList, tableList.get(tablePosition), checkList, (int) (lineStartPositon + tableColumnValues.get(tableName)), tablePosition, (int) (lineStartPositon + tableColumnValues.get(tableName))));
        }
        result.put(idValue, columnValuePair);
    }

    private void traverseJsonIntoNextLevel(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, String idValue) {
        JSONObject valueContains = result.getJSONObject(idValue);
        tablePosition = tablePosition + 1;
        if (tablePosition >= tableList.size()) {
            tablePosition = tableList.size();
        }
        parseJsonResult(valueContains.getJSONObject(tableList.get(tablePosition)), line, tableColumnValues, tableList, tableList.get(tablePosition), checkList, (int) (lineStartPositon + tableColumnValues.get(tableName)), tablePosition, (int) (lineStartPositon + tableColumnValues.get(tableName)));
    }

    private void parseLineaAndInsertIntoJson(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, JSONObject columnValuePair, String idValue) {
        createColumnValuePair(line, tableColumnValues, tableName, lineStartPositon, columnValuePair);
        if (checkList.size() != tableList.size()) {
            tablePosition = tablePosition + 1;
            columnValuePair.put(tableList.get(tablePosition), new JSONObject());
            columnValuePair.put(tableList.get(tablePosition), parseJsonResult(columnValuePair.getJSONObject(tableList.get(tablePosition)), line, tableColumnValues, tableList, tableList.get(tablePosition), checkList, (int) (lineStartPositon + tableColumnValues.get(tableName)), tablePosition, (int) (lineStartPositon + tableColumnValues.get(tableName))));
        }
        result.put(idValue, columnValuePair);
    }

    private void createColumnValuePair(String[] line, Map<String, Long> tableColumnValues, String tableName, int lineStartPositon, JSONObject columnValuePair) {
        List<String> tempHeader = headerList.stream().filter(header -> header.startsWith(tableName + ".")).collect(Collectors.toList());
        /*System.out.println(Arrays.asList((Arrays.asList(line).subList(lineStartPositon, ((int) (lineStartPositon + tableColumnValues.get(tableName))))).toString()));
        System.out.println(Arrays.asList((Arrays.asList(line).subList(lineStartPositon, ((int) (lineStartPositon + tableColumnValues.get(tableName))))).toString().replace("[", "").replace("]", "").split("|")));
        List<String> values = Arrays.asList((Arrays.asList(line).subList(lineStartPositon, ((int) (lineStartPositon + tableColumnValues.get(tableName))))).toString().replace("[", "").replace("]", "").split("|"));
        */
        List<String> values = Arrays.asList(line).subList(lineStartPositon, ((int) (lineStartPositon + tableColumnValues.get(tableName))));
        for (int j = 0; j < tempHeader.size(); j++) {
            if (j > values.size() - 1) {
                columnValuePair.put(tempHeader.get(j), "");
            } else {
                columnValuePair.put(tempHeader.get(j), values.get(j));
            }
        }
    }

    private String getUniqueValue(String[] line, List<Integer> positionsList) {

        List<String> positionPrimaryKeyValues = new ArrayList<>();
        for (Integer position : positionsList) {
            positionPrimaryKeyValues.add(line[position]);
        }
        return String.join("_", positionPrimaryKeyValues);
    }


}
