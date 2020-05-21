package com.p3.archon.sip_process.parser;

import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.bean.RecordData;
import com.p3.archon.sip_process.bean.ReportBean;
import com.p3.archon.sip_process.core.SipCreator;
import com.p3.archon.sip_process.utility.Utility;
import lombok.SneakyThrows;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Types;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.p3.archon.sip_process.constants.SipPerformanceConstant.*;

/**
 * Created by Suriyanarayanan K
 * on 04/05/20 4:41 PM.
 */
public class SipIntermediateJsonToXmlParser {


    private String NEW_LINE = "\n";
    private String outputLocation;
    private List<String> fileNamesList = new ArrayList<>();
    private String mainTable;
    private JSONObject mergedJsonObject;
    private InputArgs inputBean;


    //sip data and pdi inputs
    private StringBuffer XML_STRING = new StringBuffer();
    private SipCreator sipCreator;
    private Map<String, Integer> columnDataTypeMap;
    List<String> attachmentFiles;
    private String attachmentFileLocation;
    ReportBean reportBean;

    private String NULL_VALUE = "NULL VALUE";

    @SneakyThrows
    public SipIntermediateJsonToXmlParser(InputArgs inputBean, SipCreator sipCreator, Map<String, Integer> columnDataTypeMap, ReportBean reportBean, boolean isSingleTable) {
        this.inputBean = inputBean;
        this.outputLocation = inputBean.getOutputLocation();
        this.columnDataTypeMap = columnDataTypeMap;
        this.mainTable = inputBean.getMainTable();
        this.sipCreator = sipCreator;
        this.attachmentFileLocation = inputBean.getOutputLocation() + File.separator + ATTACHMENT_FOLDER + File.separator;
        this.reportBean = reportBean;
        this.attachmentFiles = new ArrayList<>();
        if (!isSingleTable) {
            getIntermediateFileList();
        }
    }

    private void getIntermediateFileList() {
        File outputDirectory = new File(outputLocation);
        File[] filesList = outputDirectory.listFiles((dir, name) -> name.startsWith(INTERMEDIATE_JSON_FILE) && name.toLowerCase().endsWith(JSON));
        for (File files : filesList) {
            fileNamesList.add(files.getName());
        }
    }

    protected String getXmlValidChar(String data) {
        if (data == null)
            return null;
        return data.replace("&", "&amp;").replace(">", "&gt;").replace("<", "&lt;");
    }

    public void joinAllJson() {
        Collections.sort(fileNamesList);
        String mainFileName = fileNamesList.get(0);
        String mainFileLines = Utility.readLineByLine(outputLocation + File.separator + mainFileName);
        mergedJsonObject = new JSONObject(mainFileLines);
        JSONObject rootTableJson = mergedJsonObject.getJSONObject(mainTable);
        for (int i = 1; i < fileNamesList.size(); i++) {
            String childFileName = fileNamesList.get(i);
            JSONObject childJsonObject = new JSONObject(Utility.readLineByLine(outputLocation + File.separator + childFileName));
            JSONObject anotherRootTableJson = childJsonObject.getJSONObject(mainTable);
            parseMainJsonAndMergeChildJson(rootTableJson, anotherRootTableJson, mainTable);
        }
    }

    private void parseMainJsonAndMergeChildJson(JSONObject rootTableJson, JSONObject anotherRootTableJson, String mainTable) {

        Set<String> rootTableObjectRowSet = rootTableJson.keySet();
        Set<String> anotherRootTableObjectRowSet = anotherRootTableJson.keySet();
        Set<String> containsRootTableObjectRowSet = anotherRootTableObjectRowSet.stream().filter(rowValue -> rootTableObjectRowSet.contains(rowValue)).collect(Collectors.toSet());
        updateAllContainingElements(rootTableJson, anotherRootTableJson, mainTable, containsRootTableObjectRowSet);
        updateAllRemainingRowElements(rootTableJson, anotherRootTableJson, rootTableObjectRowSet, anotherRootTableObjectRowSet);
    }

    private void updateAllContainingElements(JSONObject rootTableJson, JSONObject anotherRootTableJson, String mainTable, Set<String> containsRootTableObjectRowSet) {
        for (String containRowValue : containsRootTableObjectRowSet) {

            JSONObject rootTableRowJsonElements = rootTableJson.getJSONObject(containRowValue);
            JSONObject anotherRootTableRowJsonElements = anotherRootTableJson.getJSONObject(containRowValue);
            Set<String> rootTableRowJsonElementsSet = getRowElementsSet(mainTable, rootTableRowJsonElements);
            Set<String> anotherRootTableRowJsonElementsSet = getRowElementsSet(mainTable, anotherRootTableRowJsonElements);

            if (!rootTableRowJsonElementsSet.isEmpty() || !anotherRootTableRowJsonElementsSet.isEmpty()) {
                Set<String> remainingRootTableRowJsonElementsSet = getRemainingElements(rootTableRowJsonElementsSet, anotherRootTableRowJsonElementsSet);
                if (remainingRootTableRowJsonElementsSet.isEmpty()) {
                    traversalIntoNextLevel(rootTableRowJsonElements, anotherRootTableRowJsonElements, anotherRootTableRowJsonElementsSet);
                } else {
                    for (String remainingRowElements : remainingRootTableRowJsonElementsSet) {
                        rootTableRowJsonElements.put(remainingRowElements, anotherRootTableRowJsonElements.getJSONObject(remainingRowElements));
                    }
                }
            }
        }
    }

    private void updateAllRemainingRowElements(JSONObject rootTableJson, JSONObject anotherRootTableJson, Set<String> rootTableObjectRowSet, Set<String> anotherRootTableObjectRowSet) {
        Set<String> remainingRootTableObjectRowSet = getRemainingElements(rootTableObjectRowSet, anotherRootTableObjectRowSet);
        for (String remainingRow : remainingRootTableObjectRowSet) {
            rootTableJson.put(remainingRow, anotherRootTableJson.get(remainingRow));
        }
    }

    private void traversalIntoNextLevel(JSONObject rootTableRowJsonElements, JSONObject anotherRootTableRowJsonElements, Set<String> anotherRootTableRowJsonElementsSet) {
        if (!anotherRootTableRowJsonElementsSet.isEmpty()) {
            for (String remainingRowElements : anotherRootTableRowJsonElementsSet) {
                if (rootTableRowJsonElements.has(remainingRowElements) && anotherRootTableRowJsonElements.has(remainingRowElements)) {
                    parseMainJsonAndMergeChildJson(rootTableRowJsonElements.getJSONObject(remainingRowElements), anotherRootTableRowJsonElements.getJSONObject(remainingRowElements), remainingRowElements);
                }
            }
        }
    }

    private Set<String> getRowElementsSet(String mainTable, JSONObject rootTableRowJsonElements) {
        return rootTableRowJsonElements.keySet().stream().filter(getWithoutRootName(mainTable)).collect(Collectors.toSet());
    }

    private Set<String> getRemainingElements(Set<String> rootTableObjectRowSet, Set<String> anotherRootTableObjectRowSet) {
        Set<String> remainingRootTableObjectRowSet = new LinkedHashSet<>();
        remainingRootTableObjectRowSet.addAll(anotherRootTableObjectRowSet);
        remainingRootTableObjectRowSet.removeAll(rootTableObjectRowSet);
        return remainingRootTableObjectRowSet;
    }

    private Predicate<String> getWithoutRootName(String mainTable) {
        return column -> !column.contains(mainTable + ".");
    }


    @SneakyThrows
    public Map<String, Long> createXMlDocument(Map<String, Long> timeMaintainer) {

        File file = new File(Utility.getFileName(outputLocation, FINAL_JSON_FILE, 0, JSON));
        if (file.exists()) {
            file.delete();
        }
        PrintWriter finalJsonWriter = new PrintWriter(Utility.getFileName(outputLocation, FINAL_JSON_FILE, 0, JSON));
        Utility.fileCreatorWithContent(finalJsonWriter, mergedJsonObject.toString());
        return JsonToXmlParser(mergedJsonObject.getJSONObject(mainTable), mainTable, timeMaintainer);
    }

    private Map<String, Long> JsonToXmlParser(JSONObject mergedJsonObject, String tableName, Map<String, Long> timeMaintainer) throws IOException {

        if (!mainTable.equalsIgnoreCase(tableName)) {
            XML_STRING.append("<TABLE_" + tableName.toUpperCase() + ">" + NEW_LINE);
        }
        for (String rowValue : Utility.getSetToList(mergedJsonObject.keySet())) {
            parseTableRowElements(mergedJsonObject, tableName, rowValue, timeMaintainer);
            if (mainTable.equalsIgnoreCase(tableName)) {
                return getMainTableRowEnd(timeMaintainer);
            }
        }
        if (!mainTable.equalsIgnoreCase(tableName)) {
            XML_STRING.append("</TABLE_" + tableName.toUpperCase() + ">" + NEW_LINE);
        }
        return new LinkedHashMap<>();
    }

    private Map<String, Long> getMainTableRowEnd(Map<String, Long> timeMaintainer) throws IOException {

        timeMaintainer.put(SINGLE_SIP_RECORD_TIME, (System.currentTimeMillis() - timeMaintainer.get(SINGLE_SIP_RECORD_TIME)));
        long batchAssemblerAddTime = System.currentTimeMillis();
        sipCreator.getBatchAssembler().add(RecordData.builder().data(XML_STRING.toString()).attachmentFiles(attachmentFiles).build());
        batchAssemblerAddTime = System.currentTimeMillis() - batchAssemblerAddTime;
        timeMaintainer.put(BATCH_ASSEMBLER_TIME, batchAssemblerAddTime);
        XML_STRING.delete(0, XML_STRING.length());
        return timeMaintainer;
    }

    private void parseTableRowElements(JSONObject mergedJsonObject, String tableName, String rowValue, Map<String, Long> timeMaintainer) throws IOException {
        XML_STRING.append("<" + tableName.toUpperCase() + "_ROW>" + NEW_LINE);
        JSONObject rowJsonObject = mergedJsonObject.getJSONObject(rowValue);
        List<String> relationShipTablesList = getRelationShipTableAndCreateColumns(rowJsonObject, Utility.getSetToList(rowJsonObject.keySet()));
        Collections.sort(relationShipTablesList);
        for (String relatedTable : relationShipTablesList) {
            if (!rowJsonObject.getJSONObject(relatedTable).isEmpty()) {
                JsonToXmlParser(rowJsonObject.getJSONObject(relatedTable), relatedTable, timeMaintainer);
            }
        }
        XML_STRING.append("</" + tableName.toUpperCase() + "_ROW>" + NEW_LINE);
    }

    private List<String> getRelationShipTableAndCreateColumns(JSONObject rowJsonObject, List<String> sortedColumnWithRelationShipKeys) {
        List<String> relationShipTablesList = new ArrayList<>();
        sortedColumnWithRelationShipKeys.forEach(
                columnKeys -> {
                    if (rowJsonObject.get(columnKeys) instanceof JSONObject) {
                        relationShipTablesList.add(columnKeys);
                    } else {
                        createElement(columnKeys.split("\\.")[1], rowJsonObject.getString(columnKeys).trim(), columnDataTypeMap.get(columnKeys));
                    }
                }
        );
        return relationShipTablesList;
    }

    private void createElement(String columnName, String columnValue, int type) {

        if (!columnValue.equalsIgnoreCase(NULL_VALUE)) {
            if (type == Types.TIME_WITH_TIMEZONE || type == Types.TIMESTAMP_WITH_TIMEZONE ||
                    type == Types.TIMESTAMP) {
                writeDateKindData(columnName, columnValue);
            } else if (type == Types.DATE || columnName.equalsIgnoreCase("DATE")) {
                if (inputBean.isShowDatetime()) {
                    writeDateKindData(columnName, columnValue);
                } else {
                    writeNormalData(columnName, columnValue);
                }
            } else if (type == Types.BLOB || type == Types.LONGVARBINARY || type == Types.VARBINARY) {
                String fileLoaction = attachmentFileLocation + columnValue;
                File oldName = new File(fileLoaction);
                File newName = new File(fileLoaction.substring(0, fileLoaction.lastIndexOf(File.separator)) + File.separator + UUID.randomUUID().toString());
                oldName.renameTo(newName);
                attachmentFiles.add(newName.getAbsolutePath());
                writeNormalData(columnName, newName.getName());
            } else if (type == Types.CLOB) {
                writeNormalData(columnName, columnValue);
            } else {
                writeNormalData(columnName, getXmlValidChar(columnValue));
            }
        }
    }

    private void writeNormalData(String columnName, String columnValue) {
        XML_STRING.append("<" + columnName.toUpperCase() + ">" + columnValue + "</" + columnName.toUpperCase() + ">" + NEW_LINE);
    }

    private void writeDateKindData(String columnName, String columnValue) {
        writeNormalData(columnName, columnValue);

        XML_STRING.append("<" + columnName.toUpperCase() + "_DT_COMPATIBLE createdBy=\"DL\"" + ">" + columnValue.split("\\s+")[0].trim() + "T" + columnValue.split("\\s+")[1].substring(0, 8).trim() + "</" + columnName.toUpperCase() + "_DT_COMPATIBLE>" + NEW_LINE);
    }


    public Map<String, Long> createXMlDocumentForSingleTable(Map<String, String> sortedHeaderAndValues, Map<String, Long> timeMaintainer) throws IOException {
        XML_STRING.append("<" + inputBean.getMainTable().toUpperCase() + "_ROW>" + NEW_LINE);
        for (String columnName : sortedHeaderAndValues.keySet()) {
            createElement(columnName, sortedHeaderAndValues.get(columnName), columnDataTypeMap.get(columnName));
        }
        XML_STRING.append("</" + inputBean.getMainTable().toUpperCase() + "_ROW>" + NEW_LINE);
        return getMainTableRowEnd(timeMaintainer);
    }
}
