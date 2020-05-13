package com.p3.archon.sip_process.processor;

import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.bean.IntermediateJsonBean;
import com.p3.archon.sip_process.core.ConnectionChecker;
import com.p3.archon.sip_process.core.PdiSchemaGeneratorRussianDoll;
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


        /**
         *  Responsible for getting All Primary Column Names and Values
         */
        Connection connection = connectionChecker.checkConnection(inputBean);
        List<String> mainTablePrimaryKeyColumns = getMainTablePrimaryKeyColumnsList(connection);
        List<String> mainTableRowPrimaryValues = getMainTablePrimaryKeyColumnsValues(pathandQueryCreator, connection, mainTablePrimaryKeyColumns);


        /**
         *  Responsible for generating Record.Txt file for All Paths.
         */

        Map<Integer, Long> fileAndLinesMaintainer = new LinkedHashMap<>();
        Map<Integer, IntermediateJsonBean> intermediateJsonBeanMap = getStartGeneratingRecordFileAndCreateIntermediateJsonBean(pathTableList, queryList, mainTablePrimaryKeyColumns, fileAndLinesMaintainer, customThreadPool);


        /**
         *  Responsible for iterating Each Main Table Record and Add Into Batch Assembler
         */
        startIterateEachMainTableRecord(mainTablePrimaryKeyColumns, mainTableRowPrimaryValues, fileAndLinesMaintainer, intermediateJsonBeanMap);

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

    private void startIterateEachMainTableRecord(List<String> mainTablePrimaryKeyColumns, List<String> mainTableRowPrimaryValues, Map<Integer, Long> fileAndLinesMaintainer, Map<Integer, IntermediateJsonBean> intermediateJsonBeanMap) {
        SipCreator sipCreator = new SipCreator(inputBean);
        int rowCounter = 1;
        try {


            for (String mainTableKeyRowsValue : mainTableRowPrimaryValues) {
                LOGGER.debug(rowCounter + " : Row Start Processing");
                customThreadPool.submit(() -> {
                    intermediateJsonBeanMap.entrySet().parallelStream().forEach(
                            integerIntermediateJsonBeanEntry -> {
                                startGeneratingIntermediateJsonBasedOnMainTableValues(mainTablePrimaryKeyColumns, fileAndLinesMaintainer, intermediateJsonBeanMap, mainTableKeyRowsValue, integerIntermediateJsonBeanEntry);
                            }
                    );
                }).get();
                startAddingRecordIntoBatchAssembler(sipCreator);
                LOGGER.debug(rowCounter + " : Row Processed");
                rowCounter++;
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

    private void startGeneratingIntermediateJsonBasedOnMainTableValues(List<String> mainTablePrimaryKeyColumns, Map<Integer, Long> fileAndLinesMaintainer, Map<Integer, IntermediateJsonBean> intermediateJsonBeanMap, String mainTableKeyRowsValue, Map.Entry<Integer, IntermediateJsonBean> integerIntermediateJsonBeanEntry) {
        IntermediateJsonBean intermediateBean = intermediateJsonBeanMap.get(integerIntermediateJsonBeanEntry.getKey());
        SipIntermediateJsonParser sipIntermediateJsonParser = new SipIntermediateJsonParser(intermediateBean.getTestFileLocation(),
                intermediateBean.getTableColumnCount(),
                intermediateBean.getHeaderList(),
                intermediateBean.getTablePrimaryHeaderPosition(),
                integerIntermediateJsonBeanEntry.getKey(),
                inputBean.getOutputLocation(),
                mainTablePrimaryKeyColumns.size());
        long endLineCount = sipIntermediateJsonParser.startParsing(mainTableKeyRowsValue,
                fileAndLinesMaintainer.get(integerIntermediateJsonBeanEntry.getKey()));
        fileAndLinesMaintainer.put(integerIntermediateJsonBeanEntry.getKey(), endLineCount);
    }

    private Map<Integer, IntermediateJsonBean> getStartGeneratingRecordFileAndCreateIntermediateJsonBean(Map<Integer, List<String>> pathTableList, Map<Integer, String> queryList, List<String> mainTablePrimaryKeyColumns, Map<Integer, Long> fileAndLinesMaintainer, ForkJoinPool customThreadPool) throws ExecutionException, InterruptedException {
        Map<Integer, IntermediateJsonBean> intermediateJsonBeanMap = new LinkedHashMap<>();
        String ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN = " ORDER BY " + String.join(",", mainTablePrimaryKeyColumns);
        customThreadPool.submit(() -> {
            queryList.entrySet().parallelStream().forEach(
                    query -> {
                        startGeneratingRecordFile(pathTableList, fileAndLinesMaintainer, intermediateJsonBeanMap, ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, query);

                    });
        }).get();
        return intermediateJsonBeanMap;
    }

    private void startGeneratingRecordFile(Map<Integer, List<String>> pathTableList, Map<Integer, Long> fileAndLinesMaintainer, Map<Integer, IntermediateJsonBean> intermediateJsonBeanMap, String ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, Map.Entry<Integer, String> query) {
        LOGGER.info(query.getKey() + ": Path Record File Created Successfully Started");
        try {
            intermediateJsonBeanMap.put(query.getKey(), runQueryAndCreateIntermediateJson(query.getKey(), query.getValue() + ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, pathTableList.get(query.getKey())));
        } catch (SQLException e) {
            LOGGER.info(query.getKey() + ": Path Record File Created Successfully Started");
            e.printStackTrace();
        }
        fileAndLinesMaintainer.put(query.getKey(), (long) 0);
        LOGGER.info(query.getKey() + ": Path Record File Created Successfully Completed");
    }

    private List<String> getMainTablePrimaryKeyColumnsValues(PathandQueryCreator pathandQueryCreator, Connection
            connection, List<String> mainTablePrimaryKeyColumns) {
        List<String> mainTableTableRowPrimaryValues = new ArrayList<>();
        try (
                Statement statement = connection.createStatement();
        ) {
            String QUERY_STRING = " SELECT " + getMainTablePrimaryColumnQuery(mainTablePrimaryKeyColumns) + " from " +
                    inputBean.getSchemaName() + "." + inputBean.getMainTable() + pathandQueryCreator.getMainTableFilterQuery();
            ResultSet mainTableResultSet = statement.executeQuery(QUERY_STRING);
            ResultSetMetaData mainTableResultSetMetaData = mainTableResultSet.getMetaData();
            while (mainTableResultSet.next()) {
                List<String> rowObject = getRowObjectList(mainTableResultSet, mainTableResultSetMetaData);
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
    private IntermediateJsonBean runQueryAndCreateIntermediateJson(int fileCounter, String
            query, List<String> tableList) throws SQLException {
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
            while (resultSet.next()) {
                LOGGER.debug(row + " : Row Writing into Record File");
                List<String> rowObject = getRowObjectList(resultSet, resultSetMetaData);
                recordFileWriter.append(String.join("\\|", rowObject) + "\n");
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
        return IntermediateJsonBean.builder().headerList(headerList).tableColumnCount(tableColumnCount).tablePrimaryHeaderPosition(tablePrimaryHeaderPosition).testFileLocation(testFileLocation).build();
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


    private List<String> getRowObjectList(ResultSet resultSet, ResultSetMetaData resultSetMetaData) throws
            SQLException {
        List<String> rowObject = new ArrayList<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            int type = resultSetMetaData.getColumnType(i);
            if (type == Types.VARCHAR || type == Types.CHAR) {
                rowObject.add(String.valueOf(resultSet.getString(i)));
            } else if (type == Types.TINYINT || type == Types.NUMERIC || type == Types.BIGINT) {
                rowObject.add(String.valueOf(resultSet.getLong(i)));
            } else if (type == Types.DECIMAL || type == Types.DOUBLE) {
                rowObject.add(String.valueOf(resultSet.getDouble(i)));
            } else if (type == Types.TIME_WITH_TIMEZONE || type == Types.TIMESTAMP_WITH_TIMEZONE ||
                    type == Types.TIMESTAMP) {
                rowObject.add(resultSet.getTimestamp(i) == null ? "NULL" : resultSet.getTimestamp(i).toString());
            } else if (type == Types.TIME) {
                rowObject.add(String.valueOf(resultSet.getTime(i)));
            } else if (type == Types.DATE) {
                rowObject.add(String.valueOf(resultSet.getDate(i)));
            } else {
                rowObject.add(String.valueOf(resultSet.getString(i)));
            }
        }
        return rowObject;
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
