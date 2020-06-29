package com.p3.archon.sip_process.parser;

import com.p3.archon.sip_process.bean.*;
import com.p3.archon.sip_process.core.SipCreator;
import com.p3.archon.sip_process.utility.Utility;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;
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
    private TablewithRelationBean tablewithRelationBean;

    //sip data and pdi inputs
    private StringBuffer XML_STRING = new StringBuffer();
    private SipCreator sipCreator;
    private Map<String, Integer> columnDataTypeMap;
    List<String> attachmentFiles;
    private String attachmentFileLocation;
    ReportBean reportBean;
    private Map<String, PrintWriter> tablePrintWriter;
    private Map<String, Dictionary> tableIdsValueSet = new HashMap<>();
    private boolean isSingleTable;
    Map<String, Dictionary> tableDictionary;
    private long writerWritingTime;

    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    @SneakyThrows
    public SipIntermediateJsonToXmlParser(InputArgs inputBean, SipCreator sipCreator, Map<String, Integer> columnDataTypeMap,
                                          ReportBean reportBean, boolean isSingleTable, TablewithRelationBean tablewithRelationBean,
                                          Map<String, PrintWriter> tablePrintWriter, Map<String, Dictionary> tableDictionary) {
        this.inputBean = inputBean;
        this.outputLocation = inputBean.getOutputLocation();
        this.columnDataTypeMap = columnDataTypeMap;
        this.mainTable = inputBean.getMainTable();
        this.sipCreator = sipCreator;
        this.attachmentFileLocation = inputBean.getOutputLocation() + File.separator + ATTACHMENT_FOLDER + File.separator;
        this.reportBean = reportBean;
        this.attachmentFiles = new ArrayList<>();
        this.tablewithRelationBean = tablewithRelationBean;
        this.tablePrintWriter = tablePrintWriter;
        this.isSingleTable = isSingleTable;
        this.tableDictionary = tableDictionary;
        this.writerWritingTime = 0;
        initializeTableIdsValueSet();
        if (!isSingleTable) {
            getIntermediateFileList();
        }
    }

    private void initializeTableIdsValueSet() {
        tablePrintWriter.keySet().forEach(
                tableName -> {
                    tableIdsValueSet.put(tableName, new Hashtable());
                }
        );
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

        if (!isSingleTable) {
            fileNamesList.clear();
            getIntermediateFileList();
        }
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
        return columnName -> !columnName.contains(mainTable + ".");
    }
    @SneakyThrows
    public Map<String, Long> createXMlDocument(Map<String, Long> timeMaintainer) {
        File file = new File(Utility.getFileName(outputLocation, FINAL_JSON_FILE, 0, JSON));
        if (file.exists()) {
            file.delete();
        }
        PrintWriter finalJsonWriter = new PrintWriter(Utility.getFileName(outputLocation, FINAL_JSON_FILE, 0, JSON));
        Utility.fileCreatorWithContent(finalJsonWriter, mergedJsonObject.toString());
        return JsonToXmlParser(mergedJsonObject.getJSONObject(mainTable), getModifiedTableNameBasedOnTableName(mainTable), timeMaintainer);
    }
    private Map<String, Long> JsonToXmlParser(JSONObject mergedJsonObject, TableDetails tableName, Map<String, Long> timeMaintainer) throws IOException {
        if (!mainTable.equalsIgnoreCase(tableName.getName())) {
            XML_STRING.append("<TABLE_" + tableName.getModifiedName().toUpperCase() + ">" + NEW_LINE);
        }
        for (String rowValue : Utility.getSetToList(mergedJsonObject.keySet())) {
            if (inputBean.isIdsFile()) {
                long writerTime = System.currentTimeMillis();
                appendValuesIntoIdsFile(tableName, rowValue);
                reportBean.setIdsFileWritingTime(reportBean.getIdsFileWritingTime() + (System.currentTimeMillis() - writerTime));
            }
            parseTableRowElements(mergedJsonObject, tableName, rowValue, timeMaintainer);
            if (mainTable.equalsIgnoreCase(tableName.getName())) {
                return getMainTableRowEnd(timeMaintainer);
            }
        }
        if (!mainTable.equalsIgnoreCase(tableName.getName())) {
            XML_STRING.append("</TABLE_" + tableName.getModifiedName().toUpperCase() + ">" + NEW_LINE);
        }
        return new LinkedHashMap<>();
    }
    private void appendValuesIntoIdsFile(TableDetails tableName, String rowValue) {
        List<String> idsFileString = new ArrayList<>();
        List<String> valuesList = Arrays.asList(rowValue.split("_"));
        List<String> joinHeaderList = tableName.getKeyColumns();
        for (int i = 0; i < joinHeaderList.size(); i++) {
            if (valuesList.get(i).equalsIgnoreCase(EMPTY)) {
                idsFileString.add(joinHeaderList.get(i) + " = " + "\' \'");
            } else {
                idsFileString.add(joinHeaderList.get(i) + " = " + valuesList.get(i));
            }
        }
        if (!tableName.getName().equalsIgnoreCase(mainTable)) {
            String idsFileInsertionString = String.join(" and ", idsFileString);
            tablePrintWriter.get(tableName.getName()).write(idsFileInsertionString + NEW_LINE);
        }
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
    private void parseTableRowElements(JSONObject mergedJsonObject, TableDetails tableName, String rowValue, Map<String, Long> timeMaintainer) throws IOException {
        XML_STRING.append("<" + tableName.getModifiedName().toUpperCase() + "_ROW>" + NEW_LINE);
        JSONObject rowJsonObject = mergedJsonObject.getJSONObject(rowValue);
        List<String> relationShipTablesList = getRelationShipTableAndCreateColumns(tableName.getName(), rowJsonObject, Utility.getSetToList(rowJsonObject.keySet()));
        Collections.sort(relationShipTablesList);
        for (String relatedTable : relationShipTablesList) {
            if (!rowJsonObject.getJSONObject(relatedTable).isEmpty()) {
                JsonToXmlParser(rowJsonObject.getJSONObject(relatedTable), getModifiedTableNameBasedOnTableName(relatedTable), timeMaintainer);
            }
        }
        XML_STRING.append("</" + tableName.getModifiedName().toUpperCase() + "_ROW>" + NEW_LINE);
    }

    private List<String> getRelationShipTableAndCreateColumns(String tableName, JSONObject rowJsonObject, List<String> sortedColumnWithRelationShipKeys) {
        List<String> relationShipTablesList = new ArrayList<>();
        sortedColumnWithRelationShipKeys.forEach(
                columnKeys -> {
                    if (rowJsonObject.get(columnKeys) instanceof JSONObject) {
                        relationShipTablesList.add(columnKeys);
                    } else {
                        TableDetails tableDetails = tablewithRelationBean.getTableList().stream().filter(table -> table.getName().equalsIgnoreCase(tableName)).findFirst().get();
                        if (!tableDetails.getExtraColumns().contains(columnKeys)) {
                            String modifiedColumnName = "";
                            if (tableDetails.getOldModifyColumn().containsKey(columnKeys.split("\\.")[1].trim())) {
                                modifiedColumnName = tableDetails.getOldModifyColumn().get(columnKeys.split("\\.")[1]);
                            }
                            createElement(Utility.getTextFormatted(modifiedColumnName), rowJsonObject.getString(columnKeys).trim(), columnDataTypeMap.get(columnKeys));
                        }
                    }
                }
        );
        return relationShipTablesList;
    }

    private void createElement(String columnName, String columnValue, int type) {

        if (!columnValue.equalsIgnoreCase(NULL_VALUE)) {
            String originalColumnValue = columnValue.replace(NEW_LINE_TAG, NEW_LINE).replace(TAB_TAG, TAB);
            if (type == Types.TIME_WITH_TIMEZONE || type == Types.TIMESTAMP_WITH_TIMEZONE ||
                    type == Types.TIMESTAMP) {
                writeDateKindData(columnName, originalColumnValue);
            } else if (type == Types.DATE || columnName.equalsIgnoreCase("DATE")) {
                if (inputBean.isShowDatetime()) {
                    writeDateKindData(columnName, originalColumnValue);
                } else {
                    writeNormalData(columnName, originalColumnValue);
                }
            } else if (type == Types.BLOB || type == Types.LONGVARBINARY || type == Types.VARBINARY) {
                String fileLocation = attachmentFileLocation + originalColumnValue;
                File oldName = new File(fileLocation);
                File newName = new File(fileLocation.substring(0, fileLocation.lastIndexOf(File.separator)) + File.separator + UUID.randomUUID().toString());
                oldName.renameTo(newName);
                attachmentFiles.add(newName.getAbsolutePath());
                writeNormalData(columnName, newName.getName());
            } else if (type == Types.CLOB) {
                String clobData = originalColumnValue;
                writeNormalData(columnName, clobData);
            } else {
                writeNormalData(columnName, getXmlValidChar(originalColumnValue));
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
            createElement(Utility.getTextFormatted(columnName.split("\\.")[1]), sortedHeaderAndValues.get(columnName), columnDataTypeMap.get(columnName));
        }
        XML_STRING.append("</" + inputBean.getMainTable().toUpperCase() + "_ROW>" + NEW_LINE);
        return getMainTableRowEnd(timeMaintainer);
    }

    public TableDetails getModifiedTableNameBasedOnTableName(String tableName) {
        return tablewithRelationBean.getTableList().stream().filter(tableDetails -> tableDetails.getName().equalsIgnoreCase(tableName)).findFirst().get();
    }

    public void generateIdsFileCreation() {

        if (inputBean.isIdsFile()) {
            tablePrintWriter.entrySet().forEach(tableWriter -> {
                tableWriter.getValue().flush();
                tableWriter.getValue().close();
            });
        }
    }
}
