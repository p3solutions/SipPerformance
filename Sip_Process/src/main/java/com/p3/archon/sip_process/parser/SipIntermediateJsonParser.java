package com.p3.archon.sip_process.parser;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.p3.archon.sip_process.utility.Utility;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

import static com.p3.archon.sip_process.constants.SipPerformanceConstant.*;

/**
 * Created by Suriyanarayanan K
 * on 23/04/20 12:12 PM.
 */
public class SipIntermediateJsonParser {

    private List<String> headerList = null;
    private String fileLocationWithName = null;
    private Map<String, Long> tableColumnCount = null;
    private List<String> tablesList = new ArrayList<>();
    private String mainTable;

    private Map<String, List<Integer>> tablePrimaryHeaderPosition;
    private int fileCounter;
    private String outputLocation;
    private int mainTablePrimaryKeyCount;

    private CSVReader csvReader = null;
    private Logger LOGGER = Logger.getLogger(this.getClass().getName());
    private String[] previousLine;
    CSVParser parser;

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
        initializeCSvReader();
        LOGGER.debug("Header List :" + headerList);
        LOGGER.debug("Table Primary Header Position :" + tablePrimaryHeaderPosition);
    }

    @SneakyThrows
    private void initializeCSvReader() {

        parser = new CSVParserBuilder().withSeparator('�')
                .withIgnoreQuotations(true)
                .build();

        csvReader = new CSVReaderBuilder(new FileReader(fileLocationWithName))
                .withCSVParser(parser)
                .withSkipLines(1).build();
    }

    @SneakyThrows
    public void closeCSVReader() {
        csvReader.close();
    }


    public List<String> startParsing(String mainTableRowRecordValue, List<String> previousLineRecord, boolean isAlreadyValuesContains) {
        JSONObject rootTableJson = new JSONObject();
        List<String> previousLine = parseAllLinesAndPrepareIntermediateJson(rootTableJson, mainTableRowRecordValue, previousLineRecord, isAlreadyValuesContains);
        PrintWriter intermediateJsonWriter = null;
        try {
            intermediateJsonWriter = new PrintWriter(Utility.getFileName(outputLocation, INTERMEDIATE_JSON_FILE, fileCounter, JSON));
        } catch (FileNotFoundException e) {
            LOGGER.error("IntermediateJson File Not Found : " + e.getMessage());
        }
        Utility.fileCreatorWithContent(intermediateJsonWriter, rootTableJson.toString());
        return previousLine;
    }

    private List<String> parseAllLinesAndPrepareIntermediateJson(JSONObject rootTableJson, String mainTableRowRecordValue, List<String> previousLineRecord, boolean isAlreadyValuesContains) {

        List<String> returnPreviousList = new ArrayList<>();
        try {
            rootTableJson.put(tablesList.get(0), new JSONObject());
            if (!previousLineRecord.isEmpty()) {
                String[] previousLine = new String[previousLineRecord.size()];
                previousLine = previousLineRecord.toArray(previousLine);
                parseJsonResult(rootTableJson.getJSONObject(tablesList.get(0)), previousLine, tableColumnCount, tablesList, tablesList.get(0), new ArrayList<>(), 0, 0, 0);
            }
            String[] line = null;
            do {
                if (line != null && line.length != 0) {
                    if (checkMatching(line, mainTableRowRecordValue)) {
                        parseJsonResult(rootTableJson.getJSONObject(tablesList.get(0)), line, tableColumnCount, tablesList, tablesList.get(0), new ArrayList<>(), 0, 0, 0);
                    } else {
                        returnPreviousList = Arrays.asList(line);
                        break;
                    }
                }
            } while ((line = csvReader.readNext()) != null);

        } catch (IOException | CsvValidationException e) {

            LOGGER.error("While Parsing Csv File : " + e.getMessage());
        }
        return returnPreviousList;
    }

    private boolean checkMatching(String[] line, String mainTableRowRecordValue) {
        List<String> matchingCheckingList = new ArrayList<>();
        for (int i = 0; i < mainTablePrimaryKeyCount; i++) {
            matchingCheckingList.add(line[i]);
        }
        return (mainTableRowRecordValue.equalsIgnoreCase(String.join("�", matchingCheckingList)));
    }

    private JSONObject parseJsonResult(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, int uniqueValue) {

        if (!checkList.contains(tableName)) {
            checkList.add(tableName);
            String idValue = getUniqueValue(line, tablePrimaryHeaderPosition.get(tableName));
            JSONObject columnValuePair = new JSONObject();
            if (!idValue.contains("null") && !fetchAllIsEmpty(idValue)) {
                if (result.isEmpty()) {
                    parseLineaAndInsertIntoJson(result, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, columnValuePair, idValue);
                } else {
                    parseLineInsertorUpdateJson(result, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, columnValuePair, idValue);
                }
            }else{
                columnValuePair.put(tableName, new JSONObject());
            }
        }
        return result;
    }

    private Boolean fetchAllIsEmpty(String idValue) {
        if (idValue.contains(EMPTY)) {
            List<String> temporary = Arrays.asList(idValue.split("_"));
            List<String> emptyList = temporary.stream().filter(value -> value.equalsIgnoreCase(EMPTY)).collect(Collectors.toList());
            return temporary.size() == emptyList.size();
        } else {
            return false;
        }
    }

    private void parseLineInsertorUpdateJson(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, JSONObject columnValuePair, String idValue) {

        JSONObject valueContains = fetchTraversalDetails(result, idValue, line, lineStartPositon, tableColumnCount, tableName);
        if (valueContains.length() != 0) {
            traverseJsonIntoNextLevel(valueContains, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, idValue);
        } else {
            updateJsonAndTraverseIntoNextLevel(result, valueContains, line, tableColumnValues, tableList, tableName, checkList, lineStartPositon, tablePosition, columnValuePair, idValue);
        }
    }

    private JSONObject fetchTraversalDetails(JSONObject result, String idValue, String[] line, int lineStartPositon, Map<String, Long> tableColumnValues, String tableName) {

        JSONObject nextRowObject = new JSONObject();
        if (result.has(idValue)) {
            JSONArray rowElementsArray = result.getJSONArray(idValue);
            for (int i = 0, size = rowElementsArray.length(); i < size; i++) {
                List<String> currentColumnValues = Arrays.asList(line).subList(lineStartPositon, ((int) (lineStartPositon + tableColumnValues.get(tableName))));
                JSONObject temporaryJson = rowElementsArray.getJSONObject(i);
                List<String> temporaryValues = new ArrayList<>();
                Iterator<String> keys = temporaryJson.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (temporaryJson.get(key) instanceof String) {
                        temporaryValues.add(temporaryJson.getString(key));
                    }
                }
                if ((temporaryValues.size() == currentColumnValues.size()) && temporaryValues.containsAll(currentColumnValues)) {
                    nextRowObject = temporaryJson;
                    break;
                }
            }
        }
        return nextRowObject;
    }

    private void updateJsonAndTraverseIntoNextLevel(JSONObject mainResult, JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, JSONObject columnValuePair, String idValue) {

        createColumnValuePair(line, tableColumnValues, tableName, lineStartPositon, columnValuePair, idValue);
        tablePosition = tablePosition + 1;
        if (tablePosition >= tableList.size()) {
            tablePosition = tableList.size();
        } else {
            columnValuePair.put(tableList.get(tablePosition), new JSONObject());
            columnValuePair.put(tableList.get(tablePosition), parseJsonResult(columnValuePair.getJSONObject(tableList.get(tablePosition)), line, tableColumnValues, tableList, tableList.get(tablePosition), checkList, (int) (lineStartPositon + tableColumnValues.get(tableName)), tablePosition, (int) (lineStartPositon + tableColumnValues.get(tableName))));
        }
        if (mainResult.has(idValue)) {
            mainResult.getJSONArray(idValue).put(columnValuePair);
        } else {
            JSONArray resultJsonObject = new JSONArray();
            resultJsonObject.put(columnValuePair);
            mainResult.put(idValue, resultJsonObject);
        }
    }

    private void traverseJsonIntoNextLevel(JSONObject valueContains, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPositon, int tablePosition, String idValue) {
        tablePosition = tablePosition + 1;
        if (tablePosition >= tableList.size()) {
            tablePosition = tableList.size() - 1;
        }
        if (valueContains.has(tableList.get(tablePosition)))
            parseJsonResult(valueContains.getJSONObject(tableList.get(tablePosition)), line, tableColumnValues, tableList, tableList.get(tablePosition), checkList, (int) (lineStartPositon + tableColumnValues.get(tableName)), tablePosition, (int) (lineStartPositon + tableColumnValues.get(tableName)));
    }

    private void parseLineaAndInsertIntoJson(JSONObject result, String[] line, Map<String, Long> tableColumnValues, List<String> tableList, String tableName, List<String> checkList, int lineStartPosition, int tablePosition, JSONObject columnValuePair, String idValue) {
        JSONArray resultJsonObject = new JSONArray();
        createColumnValuePair(line, tableColumnValues, tableName, lineStartPosition, columnValuePair, idValue);
        if (checkList.size() != tableList.size()) {
            tablePosition = tablePosition + 1;
            columnValuePair.put(tableList.get(tablePosition), new JSONObject());
            columnValuePair.put(tableList.get(tablePosition), parseJsonResult(columnValuePair.getJSONObject(tableList.get(tablePosition)), line, tableColumnValues, tableList, tableList.get(tablePosition), checkList, (int) (lineStartPosition + tableColumnValues.get(tableName)), tablePosition, (int) (lineStartPosition + tableColumnValues.get(tableName))));
        }
        resultJsonObject.put(columnValuePair);
        result.put(idValue, resultJsonObject);
    }

    private void createColumnValuePair(String[] line, Map<String, Long> tableColumnValues, String tableName, int lineStartPosition, JSONObject columnValuePair, String idValue) {

        List<String> tempHeader = headerList.stream().filter(header -> header.startsWith(tableName + ".")).collect(Collectors.toList());
        List<String> values = Arrays.asList(line).subList(lineStartPosition, ((int) (lineStartPosition + tableColumnValues.get(tableName))));
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
            if (line[position].isEmpty() || line[position].isBlank()) {
                positionPrimaryKeyValues.add(EMPTY);
            } else {
                positionPrimaryKeyValues.add(line[position]);
            }
        }
        return String.join("_", positionPrimaryKeyValues);
    }


}
