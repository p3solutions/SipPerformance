package com.p3.archon.sip_process.processor;

import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.core.ConnectionChecker;
import com.p3.archon.sip_process.core.PdiSchemaGeneratorRussianDoll;
import com.p3.archon.sip_process.core.RowObjectCreator;
import com.p3.archon.sip_process.core.SipCreator;
import com.p3.archon.sip_process.parser.PathandQueryCreator;
import com.p3.archon.sip_process.parser.SipIntermediateJsonParser;
import com.p3.archon.sip_process.parser.SipIntermediateJsonToXmlParser;
import com.p3.archon.sip_process.utility.Utility;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * Created by Suriyanarayanan K
 * on 22/04/20 4:04 PM.
 */


public class Processor {
    private String[] args;
    InputArgs inputBean;


    ConnectionChecker connectionChecker = new ConnectionChecker();
    ForkJoinPool customThreadPool;
    private Map<String, Integer> columnDataTypeMap = new LinkedHashMap<>();


    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public Processor(String[] args) {
        this.args = args;
        this.inputBean = InputArgs.builder().build();

    }

    /**
     * Set all Command Line variable to  InputArgs Bean
     */
    public void setValuesIntoBean() {
        CmdLineParser parser = new CmdLineParser(inputBean);
        try {
            parser.parseArgument(args);
            inputBean.validateInputs();

        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            LOGGER.error("Please check arguments specified. \n" + e.getMessage() + "\nTerminating ... ");
            System.exit(1);
        }
    }

    /**
     * Sip Extraction Started here
     */

    @SneakyThrows
    public void startExtraction() {


        /**
         * Custom Thread Pool Initiated
         * */
        customThreadPool = new ForkJoinPool(inputBean.getThreadCount());
        /**
         *  Responsible for Path and Query List
         */
        PathandQueryCreator pathandQueryCreator = new PathandQueryCreator(inputBean);
        Map<Integer, List<String>> pathTableList = pathandQueryCreator.startParsingAndCreationPathList();
        Map<Integer, String> queryList = pathandQueryCreator.getConstructQueryList(pathTableList);
        TreeMap<String, String> charReplacement = pathandQueryCreator.getCharacterReplacementMap();

        /**
         *  Responsible for getting All Primary Column Names and Values
         */
        Connection connection = connectionChecker.checkConnection(inputBean);
        List<String> mainTablePrimaryKeyColumns = getMainTablePrimaryKeyColumnsList(connection);
        List<String> mainTableRowPrimaryValues = getMainTablePrimaryKeyColumnsValues(pathandQueryCreator, connection, mainTablePrimaryKeyColumns, charReplacement);


        /**
         *  Responsible for generating Record.Txt file for All Paths.
         */

        Map<Integer, List<String>> filePreviousLinesMaintainer = new LinkedHashMap<>();
        Map<Integer, SipIntermediateJsonParser> sipIntermediateJsonParserMap = getStartGeneratingRecordFileAndCreateIntermediateJsonBean(pathTableList, queryList, mainTablePrimaryKeyColumns, filePreviousLinesMaintainer, customThreadPool, charReplacement);


        /**
         *  Responsible for iterating Each Main Table Record and Add Into Batch Assembler
         */
        startIterateEachMainTableRecord(mainTableRowPrimaryValues, filePreviousLinesMaintainer, sipIntermediateJsonParserMap);

        /**
         *  Responsible for generating Single Record based on Main Table Key Values
         */
        startCreatingPdiSchemaFile();
    }

    private void startCreatingPdiSchemaFile() throws IOException {
        PdiSchemaGeneratorRussianDoll pdiSchemaGeneratorRussianDoll = new PdiSchemaGeneratorRussianDoll(inputBean, columnDataTypeMap);
        boolean creationStatus = pdiSchemaGeneratorRussianDoll.startPdiSchemaCreation();
        if (creationStatus)
            deleteUnwantedFiles();
    }

    private void startIterateEachMainTableRecord(List<String> mainTableRowPrimaryValues, Map<Integer, List<String>> filePreviousLinesMaintainer, Map<Integer, SipIntermediateJsonParser> sipIntermediateJsonParserMap) {
        SipCreator sipCreator = new SipCreator(inputBean);
        long rowCounter = 1;
        try {
            for (String mainTableKeyRowsValue : mainTableRowPrimaryValues) {
                LOGGER.debug(rowCounter + " : Row Start Processing");
                customThreadPool.submit(() -> {
                    sipIntermediateJsonParserMap.entrySet().parallelStream().forEach(
                            sipIntermediateJsonParserEntry -> {
                                startGeneratingIntermediateJsonBasedOnMainTableValues(
                                        filePreviousLinesMaintainer,
                                        sipIntermediateJsonParserMap.get(sipIntermediateJsonParserEntry.getKey()),
                                        mainTableKeyRowsValue,
                                        sipIntermediateJsonParserEntry.getKey()
                                );
                            }
                    );
                }).get();
                startAddingRecordIntoBatchAssembler(sipCreator);
                LOGGER.debug(rowCounter + " : Row Processed");

                if (rowCounter % 10000 == 0) {
                    LOGGER.debug("======================================================");
                    LOGGER.debug("");
                    LOGGER.debug(rowCounter + " Processed");
                    LOGGER.debug("");
                    LOGGER.debug("======================================================");
                }
                rowCounter++;
            }
            for (Map.Entry<Integer, SipIntermediateJsonParser> sipIntermediateJsonParserEntry : sipIntermediateJsonParserMap.entrySet()) {
                sipIntermediateJsonParserEntry.getValue().closeCSVReader();
            }
            sipCreator.getBatchAssembler().end();
        } catch (Exception e) {
            LOGGER.error("Error :" + rowCounter);
        }
        LOGGER.info("Total No.of.Records Processed :" + (rowCounter - 1));
    }

    /**
     * Responsible for merge all the intermediate json , create sip file and delete unwanted files.
     */
    private void startAddingRecordIntoBatchAssembler(SipCreator sipCreator) {
        SipIntermediateJsonToXmlParser sipIntermediateJsonToXmlParser = new SipIntermediateJsonToXmlParser(inputBean, sipCreator, columnDataTypeMap);
        sipIntermediateJsonToXmlParser.joinAllJson();
        sipIntermediateJsonToXmlParser.createXMlDocument();
    }

    private void startGeneratingIntermediateJsonBasedOnMainTableValues(Map<Integer, List<String>> filePreviousLinesMaintainer, SipIntermediateJsonParser sipIntermediateJsonParser, String mainTableKeyRowsValue, Integer key) {
        List endLineCount = sipIntermediateJsonParser.startParsing(mainTableKeyRowsValue, filePreviousLinesMaintainer.get(key));
        filePreviousLinesMaintainer.put(key, endLineCount);
    }

    private Map<Integer, SipIntermediateJsonParser> getStartGeneratingRecordFileAndCreateIntermediateJsonBean(Map<Integer, List<String>> pathTableList, Map<Integer, String> queryList, List<String> mainTablePrimaryKeyColumns, Map<Integer, List<String>> filePreviousLinesMaintainer, ForkJoinPool customThreadPool, TreeMap<String, String> charReplacement) throws ExecutionException, InterruptedException {
        Map<Integer, SipIntermediateJsonParser> sipIntermediateJsonParserMap = new LinkedHashMap<>();
        String ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN = " ORDER BY " + String.join(",", mainTablePrimaryKeyColumns);
        customThreadPool.submit(() -> {
            queryList.entrySet().parallelStream().forEach(
                    query -> {
                        startGeneratingRecordFile(pathTableList, filePreviousLinesMaintainer, sipIntermediateJsonParserMap, ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, query, mainTablePrimaryKeyColumns, charReplacement);
                    });
        }).get();
        return sipIntermediateJsonParserMap;
    }

    private void startGeneratingRecordFile(Map<Integer, List<String>> pathTableList, Map<Integer, List<String>> filePreviousLinesMaintainer, Map<Integer, SipIntermediateJsonParser> intermediateJsonBeanMap, String ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, Map.Entry<Integer, String> query, List<String> mainTablePrimaryKeyColumns, TreeMap<String, String> charReplacement) {
        LOGGER.info(query.getKey() + ": Path Record File Created Successfully Started");
        try {
            intermediateJsonBeanMap.put(query.getKey(), runQueryAndCreateIntermediateJson(query.getKey(), query.getValue() + ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, pathTableList.get(query.getKey()), mainTablePrimaryKeyColumns, charReplacement));
        } catch (SQLException e) {
            LOGGER.info(query.getKey() + ": Path Record File Created Successfully Started");
            e.printStackTrace();
        }
        filePreviousLinesMaintainer.put(query.getKey(), new ArrayList<>());
        LOGGER.info(query.getKey() + ": Path Record File Created Successfully Completed");
    }

    private List<String> getMainTablePrimaryKeyColumnsValues(PathandQueryCreator pathandQueryCreator, Connection
            connection, List<String> mainTablePrimaryKeyColumns, TreeMap<String, String> charReplacement) {
        List<String> mainTableTableRowPrimaryValues = new ArrayList<>();
        try (
                Statement statement = connection.createStatement();
        ) {
            String QUERY_STRING = " SELECT " + getMainTablePrimaryColumnQuery(mainTablePrimaryKeyColumns) + " from " +
                    inputBean.getSchemaName() + "." + inputBean.getMainTable() + pathandQueryCreator.getMainTableFilterQuery();
            ResultSet mainTableResultSet = statement.executeQuery(QUERY_STRING);
            ResultSetMetaData mainTableResultSetMetaData = mainTableResultSet.getMetaData();
            RowObjectCreator mainTableRowObjectCreator = new RowObjectCreator(mainTableResultSetMetaData, inputBean.getOutputLocation(), charReplacement, inputBean.isShowDatetime());
            while (mainTableResultSet.next()) {
                List<String> rowObject = mainTableRowObjectCreator.getRowObjectList(mainTableResultSet);
                mainTableTableRowPrimaryValues.add(String.join(",", rowObject));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return mainTableTableRowPrimaryValues;
    }

    private List<String> getMainTablePrimaryKeyColumnsList(Connection connection) throws SQLException {
        List<String> mainTablePrimaryKeyColumns = new ArrayList<>();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        Map<String, List<String>> tablePrimaryKey = generateTablePrimaryKey(new LinkedHashMap<>(), databaseMetaData, Arrays.asList(new String[]{inputBean.getSchemaName() + "." + inputBean.getMainTable()}));
        mainTablePrimaryKeyColumns.addAll(tablePrimaryKey.get(inputBean.getMainTable()));
        return mainTablePrimaryKeyColumns;
    }

    private String getMainTablePrimaryColumnQuery(List<String> mainTablePrimaryColumn) {
        List<String> COLUMN_STRING = new ArrayList<>();
        for (String column : mainTablePrimaryColumn) {
            COLUMN_STRING.add(column + " as \"" + column + "\"");
        }
        return String.join(",", COLUMN_STRING).isEmpty() ? " * " : String.join(",", COLUMN_STRING);
    }

    /**
     * Responsible for run the query and parse the data .
     */
    private SipIntermediateJsonParser runQueryAndCreateIntermediateJson(int fileCounter, String
            query, List<String> tableList, List<String> mainTablePrimaryKeyColumns, TreeMap<String, String> charReplacement) throws SQLException {
        LOGGER.debug("QUERY  : " + query);
        int row = 0;
        String testFileLocation = Utility.getFileName(inputBean.getOutputLocation(), "records_", fileCounter, "txt");
        File recordWriterFile = new File(testFileLocation);
        List<String> headerList = null;
        Map<String, Long> tableColumnCount = null;
        Map<String, List<Integer>> tablePrimaryHeaderPosition = null;
        try (
                PrintWriter recordFileWriter = new PrintWriter(new OutputStreamWriter(
                        new BufferedOutputStream(
                                new FileOutputStream(testFileLocation)), "UTF-8"));
                Connection connection = connectionChecker.checkConnection(inputBean);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query);
        ) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            Map<String, List<String>> tablePrimaryKey = generateTablePrimaryKey(new LinkedHashMap<>(), databaseMetaData, tableList);
            headerList = getHeaderList(resultSetMetaData);
            recordFileWriter.append(String.join("\\|", headerList) + "\n");
            RowObjectCreator rowObjectCreator = new RowObjectCreator(resultSetMetaData, inputBean.getOutputLocation(), charReplacement, inputBean.isShowDatetime());
            while (resultSet.next()) {
                LOGGER.debug(row + " : Row Writing into Record File");
                List<String> rowObject = rowObjectCreator.getRowObjectList(resultSet);
                recordFileWriter.append(String.join("�", rowObject) + "\n");
                recordFileWriter.flush();
                LOGGER.debug(row + " : Row Written");
                row++;
                LOGGER.debug("File Size :" + recordWriterFile.length());
            }
            Utility.endWriter(recordFileWriter);
            tableColumnCount = getTableColumnCount(headerList);
            tablePrimaryHeaderPosition = getTablePrimaryHeaderPositionMap(tablePrimaryKey, headerList);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        SipIntermediateJsonParser sipIntermediateJsonParser = new SipIntermediateJsonParser(testFileLocation,
                tableColumnCount,
                headerList,
                tablePrimaryHeaderPosition,
                fileCounter,
                inputBean.getOutputLocation(),
                mainTablePrimaryKeyColumns.size());
        return sipIntermediateJsonParser;
    }

    private Map<String, List<Integer>> getTablePrimaryHeaderPositionMap
            (Map<String, List<String>> tablePrimaryKey, List<String> headerList) {
        Map<String, List<Integer>> tablePrimaryHeaderPosition = new LinkedHashMap<>();
        tablePrimaryKey.entrySet().forEach(
                keyValue -> {
                    List<Integer> positions = new ArrayList<>();
                    keyValue.getValue().forEach(
                            column -> {
                                positions.add(headerList.indexOf(column));
                            }
                    );
                    Collections.sort(positions);
                    tablePrimaryHeaderPosition.put(keyValue.getKey(), positions);
                }
        );
        return tablePrimaryHeaderPosition;
    }




    private Map<String, Long> getTableColumnCount(List<String> headerList) {
        Map<String, Long> tableColumnCount = new LinkedHashMap<>();
        for (String table : headerList.stream().map(header -> header.split("\\.")[0]).distinct().collect(Collectors.toList())) {
            tableColumnCount.put(table, headerList.stream().filter(header -> header.startsWith(table + ".")).count());
        }
        return tableColumnCount;
    }

    @SneakyThrows
    private List<String> getHeaderList(ResultSetMetaData resultSetMetaData) {
        List<String> headerList = new ArrayList<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            columnDataTypeMap.put(resultSetMetaData.getColumnLabel(i).trim(), resultSetMetaData.getColumnType(i));
            headerList.add(resultSetMetaData.getColumnLabel(i).trim());
        }
        return headerList;
    }

    private Map<String, List<String>> generateTablePrimaryKey
            (Map<String, List<String>> tablePrimaryKey, DatabaseMetaData databaseMetaData, List<String> tableList) throws
            SQLException {
        String schemaPattern;
        String catalog = null;
        for (String table : tableList) {
            table = table.trim();
            schemaPattern = table.split("\\.")[0];
            String tableName = table.split("\\.")[1].toLowerCase();
            ResultSet rsPK = databaseMetaData.getPrimaryKeys(catalog, schemaPattern, tableName);
            List<String> tempList = new ArrayList<>();
            while (rsPK.next()) {
                String primaryKeyColumn = rsPK.getString("COLUMN_NAME");
                tempList.add(tableName + "." + primaryKeyColumn);
            }
            tablePrimaryKey.put(table.split("\\.")[1].trim(), tempList);
        }
        return tablePrimaryKey;
    }

    public void deleteUnwantedFiles() {
        File outputFolder = new File(inputBean.getOutputLocation());
        File[] filesList = outputFolder.listFiles((dir, name) -> name.toLowerCase().endsWith(".json") || name.toLowerCase().endsWith(".txt"));
        for (File deleteFile : filesList) {
            deleteFile.delete();
        }
    }
}
