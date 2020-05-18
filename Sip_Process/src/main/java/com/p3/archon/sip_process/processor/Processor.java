package com.p3.archon.sip_process.processor;

import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.bean.PathPerformance;
import com.p3.archon.sip_process.bean.PerformanceStatisticsReport;
import com.p3.archon.sip_process.core.*;
import com.p3.archon.sip_process.parser.PathandQueryCreator;
import com.p3.archon.sip_process.parser.SipIntermediateJsonParser;
import com.p3.archon.sip_process.parser.SipIntermediateJsonToXmlParser;
import com.p3.archon.sip_process.utility.Utility;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static com.p3.archon.sip_process.constants.FileNameConstant.*;

/**
 * Created by Suriyanarayanan K
 * on 22/04/20 4:04 PM.
 */


public class Processor {


    private InputArgs inputBean;
    private ConnectionChecker connectionChecker = new ConnectionChecker();
    private ForkJoinPool customThreadPool;
    private Map<String, Integer> columnDataTypeMap = new LinkedHashMap<>();
    PerformanceStatisticsReport performanceStatisticsReport = PerformanceStatisticsReport.builder().startTime(new Date().getTime()).build();
    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public Processor() {
        this.inputBean = InputArgs.builder().build();
    }

    /**
     * Set all Command Line variable to  InputArgs Bean
     */
    public void setValuesIntoBean() {

        /**
         *
         * Config Properties Inputs
         *
         * */
        LOGGER.info("we will assume the default values if User not specify the inputs in a config.properties file . \n" +
                "please ensure the values in properties file \n" +
                "hostName               =   String\n" +
                "port                   =   Integer\n" +
                "databaseName           =   String\n" +
                "schemaName             =   String\n" +
                "userName               =   String\n" +
                "password               =   String\n" +
                "databaseServer         =   String\n" +
                "outputLocation         =   String\n" +
                "mainTable              =   String\n" +
                "sipInputJsonLocation   =   String\n" +
                "appName                =   String\n" +
                "holdName               =   String\n" +
                "recordPerXML           =   Integer\n" +
                "theadCount             =   Integer\n" +
                "pathThreadCount        =   Integer\n" +
                "showTime               =   Boolean"
        );
        PropertyFileReader propertyFileReader = new PropertyFileReader("config.properties");

        String hostName = propertyFileReader.getStringValue("hostName", "");
        int portNo = propertyFileReader.getIntegerValue("port", 0);
        String databaseName = propertyFileReader.getStringValue("databaseName", "");
        String schemaName = propertyFileReader.getStringValue("schemaName", "");
        String userName = propertyFileReader.getStringValue("userName", "");
        String password = propertyFileReader.getStringValue("password", "");
        String databaseServer = propertyFileReader.getStringValue("databaseServer", "");
        String outputLocation = propertyFileReader.getStringValue("outputLocation", "");
        String mainTable = propertyFileReader.getStringValue("mainTable", "");
        String sipInputJsonLocation = propertyFileReader.getStringValue("sipInputJsonLocation", "");
        String appName = propertyFileReader.getStringValue("appName", "");
        String holdName = propertyFileReader.getStringValue("holdName", "");
        int recordPerXML = (propertyFileReader.getIntegerValue("recordPerXML", 10));
        int theadCount = (propertyFileReader.getIntegerValue("theadCount", 10));
        int pathThreadCount = (propertyFileReader.getIntegerValue("pathThreadCount", 10));
        boolean showTime = (propertyFileReader.getBooleanValue("showTime", false));

        inputBean = InputArgs.builder()
                .hostName(hostName)
                .portNo(String.valueOf(portNo))
                .databaseName(databaseName)
                .schemaName(schemaName)
                .user(userName)
                .pass(password)
                .databaseServer(databaseServer)
                .outputLocation(outputLocation)
                .mainTable(mainTable)
                .jsonFile(sipInputJsonLocation)
                .appName(appName)
                .holdName(holdName)
                .rpx(recordPerXML)
                .threadCount(theadCount)
                .pathThreadCount(pathThreadCount)
                .showDatetime(showTime)
                .build();
        inputBean.validateInputs();


       /* CmdLineParser parser = new CmdLineParser(inputBean);
        try {
            parser.parseArgument(args);
            inputBean.validateInputs();
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            LOGGER.error("Please check arguments specified. \n" + e.getMessage() + "\nTerminating ... ");
            System.exit(1);
        }*/
    }

    /**
     * Sip Extraction Started here
     */

    @SneakyThrows
    public void startExtraction() {


        /**
         * Custom Thread Pool Initiated
         **/
        customThreadPool = new ForkJoinPool(inputBean.getThreadCount());
        /**
         *  Responsible for Path and Query List
         */
        PathandQueryCreator pathandQueryCreator = new PathandQueryCreator(inputBean);
        Map<Integer, List<String>> pathTableList = pathandQueryCreator.startParsingAndCreationPathList();
        Map<Integer, String> queryList = pathandQueryCreator.getConstructQueryList(pathTableList);
        TreeMap<String, String> charReplacement = pathandQueryCreator.getCharacterReplacementMap();

        performanceStatisticsReport.setTotalDatabaseHit(queryList.size());


        /**
         *  Responsible for getting All Primary Column Names and Values
         */
        Connection connection = connectionChecker.checkConnection(inputBean);
        List<String> mainTablePrimaryKeyColumns = getMainTablePrimaryKeyColumnsList(connection);
        List<String> mainTableRowPrimaryValues = getMainTablePrimaryKeyColumnsValues(pathandQueryCreator, connection, mainTablePrimaryKeyColumns, charReplacement);

        performanceStatisticsReport.setTotalSipRecord(mainTableRowPrimaryValues.size());


        /**
         *  Responsible for generating Record.Txt file for All Paths.
         */
        Map<Integer, List<String>> filePreviousLinesMaintainer = new LinkedHashMap<>();
        Map<Integer, SipIntermediateJsonParser> sipIntermediateJsonParserMap = getStartGeneratingRecordFileAndCreateIntermediateJsonBean(pathTableList, queryList, mainTablePrimaryKeyColumns, filePreviousLinesMaintainer, customThreadPool, charReplacement, performanceStatisticsReport.getPathPerformanceMap());

        /**
         *  Responsible for iterating Each Main Table Record and Add Into Batch Assembler
         */
        startIterateEachMainTableRecord(mainTableRowPrimaryValues, filePreviousLinesMaintainer, sipIntermediateJsonParserMap);

        /**
         *  Responsible for generating Single Record based on Main Table Key Values
         */
        startCreatingPdiSchemaFile();


        performanceStatisticsReport.setEndTime(new Date().getTime());


        displayPerformanceReport();

    }

    private void displayPerformanceReport() {


        long totalStringRecordCounter = 0;
        long totalBlobRecordCounter = 0;
        long totalClobRecordCounter = 0;

        long totalStringRecordWriteTime = 0;
        long totalBlobRecordWriteTime = 0;
        long totalClobRecordWriteTime = 0;

        long totalQueryTime = 0;
        long totalResultCount = 0;

        for (Map.Entry<Integer, PathPerformance> pathPerformanceEntry : performanceStatisticsReport.getPathPerformanceMap().entrySet()) {
            totalStringRecordCounter += pathPerformanceEntry.getValue().getNormalCounter();
            totalBlobRecordCounter += pathPerformanceEntry.getValue().getBlobCounter();
            totalClobRecordCounter += pathPerformanceEntry.getValue().getClobCounter();

            totalStringRecordWriteTime += pathPerformanceEntry.getValue().getNormalTime();
            totalBlobRecordWriteTime += pathPerformanceEntry.getValue().getBlobTime();
            totalClobRecordWriteTime += pathPerformanceEntry.getValue().getClobTime();

            totalQueryTime += pathPerformanceEntry.getValue().getQueryTime();
            totalResultCount += pathPerformanceEntry.getValue().getResultTime();

        }

        generatePerformanceReport(
                inputBean.getOutputLocation(),
                totalStringRecordCounter,
                totalClobRecordCounter,
                totalBlobRecordCounter,
                performanceStatisticsReport.getTotalDatabaseHit(),
                totalBlobRecordWriteTime,
                totalClobRecordWriteTime,
                totalStringRecordWriteTime,
                totalQueryTime,
                performanceStatisticsReport.getTotalBatchAssemblerTime(),
                performanceStatisticsReport.getTotalDatabaseHit(),
                performanceStatisticsReport.getTotalSipRecord(),
                totalResultCount
        );
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

                long writeSingleSipRecord = System.currentTimeMillis();

                LOGGER.info(rowCounter + " : Row Start Processing");
                customThreadPool.submit(() -> {
                    sipIntermediateJsonParserMap.entrySet().parallelStream().forEach(
                            sipIntermediateJsonParserEntry -> {
                                startGeneratingIntermediateJsonBasedOnMainTableValues(filePreviousLinesMaintainer, sipIntermediateJsonParserMap.get(sipIntermediateJsonParserEntry.getKey()), mainTableKeyRowsValue, sipIntermediateJsonParserEntry.getKey());
                            });
                }).get();
                performanceStatisticsReport.setTotalBatchAssemblerTime(performanceStatisticsReport.getTotalBatchAssemblerTime() + startAddingRecordIntoBatchAssembler(sipCreator));
                performanceStatisticsReport.setWriteSipRecordTIme(performanceStatisticsReport.getWriteSipRecordTIme() + (System.currentTimeMillis() - writeSingleSipRecord));
                LOGGER.info(rowCounter + " : Row Processed");
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
            LOGGER.error("Error :" + e.getMessage());
            LOGGER.error("Error :" + rowCounter);
        }
        LOGGER.debug("Total No.of.Records Processed :" + (rowCounter - 1));
    }

    /**
     * Responsible for merge all the intermediate json , create sip file and delete unwanted files.
     */
    private long startAddingRecordIntoBatchAssembler(SipCreator sipCreator) {

        SipIntermediateJsonToXmlParser sipIntermediateJsonToXmlParser = new SipIntermediateJsonToXmlParser(inputBean, sipCreator, columnDataTypeMap, performanceStatisticsReport);
        sipIntermediateJsonToXmlParser.joinAllJson();
        return sipIntermediateJsonToXmlParser.createXMlDocument();
    }

    private void startGeneratingIntermediateJsonBasedOnMainTableValues(Map<Integer, List<String>> filePreviousLinesMaintainer, SipIntermediateJsonParser sipIntermediateJsonParser, String mainTableKeyRowsValue, Integer key) {
        List endLineCount = sipIntermediateJsonParser.startParsing(mainTableKeyRowsValue, filePreviousLinesMaintainer.get(key));
        filePreviousLinesMaintainer.put(key, endLineCount);
    }

    private Map<Integer, SipIntermediateJsonParser> getStartGeneratingRecordFileAndCreateIntermediateJsonBean(Map<Integer, List<String>> pathTableList, Map<Integer, String> queryList, List<String> mainTablePrimaryKeyColumns, Map<Integer, List<String>> filePreviousLinesMaintainer, ForkJoinPool customThreadPool, TreeMap<String, String> charReplacement, Map<Integer, PathPerformance> pathPerformanceMap) throws ExecutionException, InterruptedException {
        Map<Integer, SipIntermediateJsonParser> sipIntermediateJsonParserMap = new LinkedHashMap<>();
        String ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN = " ORDER BY " + String.join(",", mainTablePrimaryKeyColumns);
        customThreadPool.submit(() -> {
            queryList.entrySet().parallelStream().forEach(
                    query -> {
                        pathPerformanceMap.put(query.getKey(), PathPerformance.builder().build());
                        startGeneratingRecordFile(pathTableList, filePreviousLinesMaintainer, sipIntermediateJsonParserMap, ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, query, mainTablePrimaryKeyColumns, charReplacement, pathPerformanceMap.get(query.getKey()));
                    });
        }).get();
        return sipIntermediateJsonParserMap;
    }

    private void startGeneratingRecordFile(Map<Integer, List<String>> pathTableList, Map<Integer, List<String>> filePreviousLinesMaintainer, Map<Integer, SipIntermediateJsonParser> intermediateJsonBeanMap, String ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, Map.Entry<Integer, String> query, List<String> mainTablePrimaryKeyColumns, TreeMap<String, String> charReplacement, PathPerformance pathPerformance) {
        LOGGER.info(query.getKey() + ": Path Record File Created Successfully Started");
        try {
            intermediateJsonBeanMap.put(query.getKey(), runQueryAndCreateIntermediateJson(query.getKey(), query.getValue() + ORDER_BY_MAIN_TABLE_PRIMARY_COLUMN, pathTableList.get(query.getKey()), mainTablePrimaryKeyColumns, charReplacement, pathPerformance));
        } catch (SQLException e) {
            LOGGER.info(query.getKey() + ": Path Record File Line Error Occurred");
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

            LOGGER.debug("Total Number of Main Table Primary Key Records :" + mainTableRowObjectCreator.getStringRecordCounter());
            LOGGER.debug("Total time taken for writing Main Table Primary Key Records" + mainTableRowObjectCreator.getTotalStringRecordWriteTime());

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
            query, List<String> tableList, List<String> mainTablePrimaryKeyColumns, TreeMap<String, String> charReplacement, PathPerformance pathPerformance) throws SQLException {
        LOGGER.debug("QUERY  : " + query);
        int row = 0;
        String testFileLocation = Utility.getFileName(inputBean.getOutputLocation(), RECORD_FILE, fileCounter, TXT);
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

        ) {
            long startTimeRec = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(query);
            pathPerformance.setQueryTime(pathPerformance.getQueryTime() + (System.currentTimeMillis() - startTimeRec));
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            Map<String, List<String>> tablePrimaryKey = generateTablePrimaryKey(new LinkedHashMap<>(), databaseMetaData, tableList);
            headerList = getHeaderList(resultSetMetaData);
            recordFileWriter.append(String.join("\\|", headerList) + "\n");
            RowObjectCreator rowObjectCreator = new RowObjectCreator(resultSetMetaData, inputBean.getOutputLocation(), charReplacement, inputBean.isShowDatetime());
            long startResultCount = 0;
            while (resultSet.next()) {
                //LOGGER.debug(row + " : Row Writing into Record File");
                List<String> rowObject = rowObjectCreator.getRowObjectList(resultSet);
                recordFileWriter.append(String.join("�", rowObject) + "\n");
                recordFileWriter.flush();
                //LOGGER.debug(row + " : Row Written");
                row++;
                //LOGGER.debug("File Size :" + recordWriterFile.length());
                startResultCount += (System.currentTimeMillis() - startResultCount);
            }
            pathPerformance.setNormalCounter(pathPerformance.getNormalCounter() + rowObjectCreator.getStringRecordCounter());
            pathPerformance.setBlobCounter(pathPerformance.getBlobCounter() + rowObjectCreator.getBlobCounter());
            pathPerformance.setClobCounter(pathPerformance.getClobCounter() + rowObjectCreator.getClobCounter());
            pathPerformance.setNormalTime(pathPerformance.getNormalTime() + rowObjectCreator.getTotalStringRecordWriteTime());
            pathPerformance.setBlobTime(pathPerformance.getBlobTime() + rowObjectCreator.getTotalBlobWriteTime());
            pathPerformance.setClobTime(pathPerformance.getClobTime() + rowObjectCreator.getTotalClobWriteTime());
            pathPerformance.setResultTime(pathPerformance.getResultTime() + startResultCount);


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
        File[] filesList = outputFolder.listFiles((dir, name) -> name.toLowerCase().endsWith("." + JSON) || name.toLowerCase().endsWith("." + TXT));
        Path rootPath = Paths.get(inputBean.getOutputLocation() + File.separator + ATTACHMENT_FOLDER);
        try {
            Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (File deleteFile : filesList) {
            deleteFile.delete();
        }
    }


    public void generatePerformanceReport(String destinationPath, long stringRecordCounter, long clobCounter, long blobCounter,
                                          long dbHitCounter, long totalBlobWriteTime, long totalClobWriteTime, long totalStringRecordWriteTime,
                                          long totalDBConnectionTime, long totalBatchAssAddTime, long recordsProcessed,
                                          long totalCreateTimeForParentRecord, long totaldbDataTime) {
        try {
            File psFile = new File(destinationPath + File.separator + "Archon_performance_statistics.html");
            if (!psFile.exists()) {
                psFile.createNewFile();
            }
            double avgClobTime = clobCounter != 0 ? ((double) totalClobWriteTime / clobCounter) / 60000 : 0f;
            double avgBlobTime = blobCounter != 0 ? ((double) totalBlobWriteTime / blobCounter) / 60000 : 0f;
            double avgOtherDataTime = stringRecordCounter != 0
                    ? ((double) totalStringRecordWriteTime / stringRecordCounter) / 60000
                    : 0f;
            double avgBatchAddTime = recordsProcessed != 0 ? ((double) totalBatchAssAddTime / totalCreateTimeForParentRecord) / 60000
                    : 0f;
            double avgParentRecordCreateTime = recordsProcessed != 0
                    ? ((double) totalCreateTimeForParentRecord / totalCreateTimeForParentRecord) / 60000
                    : 0f;
            String html1 = "<html>\r\n" + "	<head>\r\n" + "		<h2 align=\"center\">Performance Statistics</h2>\r\n"
                    + "	</head>\r\n" + "	<body>\r\n"
                    + "		<table border=\"2\" style=\"width:100%\" border-spacing=\"5px\">\r\n"
                    + "			<tr align=\"center\">\r\n" + "				<td colspan=\"3\">Process</td>\r\n"
                    + "				<td colspan=\"3\">Values</td>\r\n" + "			</tr>\r\n" + "			<tr>\r\n"
                    + "				<td colspan=\"3\">Total source database hit</td>\r\n"
                    + "				<td colspan=\"3\">" + dbHitCounter + "</td>\r\n" + "			</tr>\r\n"
                    + "<tr>\r\n" + "<tr>\r\n"
                    + "				<td colspan=\"3\">Total number of CLOB data processed</td>\r\n"
                    + "				<td colspan=\"3\">" + clobCounter + "</td>\r\n" + "			</tr>\r\n"
                    + "			<tr>\r\n"
                    + "				<td colspan=\"3\">Total number of BLOB data processed</td>\r\n"
                    + "				<td colspan=\"3\">" + blobCounter + "</td>\r\n" + "			</tr>" + "<tr>\n"
                    + "\t\t\t\t<td colspan=\"3\">Average time taken for writing one SIP record</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + avgParentRecordCreateTime + " minutes</td>\n" + "\t\t\t</tr>\n"
                    + "\t\t\t\t<td colspan=\"3\">Total time taken for writing SIP records</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + ((double) totalCreateTimeForParentRecord) / 60000
                    + " minutes</td>\n" + "\t\t\t</tr>\n" + "\t\t\t<tr>\n"
                    + "\t\t\t\t<td colspan=\"3\">Average time taken for adding one record to batch assembler</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + avgBatchAddTime + " minutes</td>\n" + "\t\t\t</tr>\n"
                    + "\t\t\t\t<td colspan=\"3\">Total time taken for adding records to batch assembler</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + ((double) totalBatchAssAddTime) / 60000 + " minutes</td>\n"
                    + "\t\t\t</tr>\n" + "\t\t\t<tr>\n"
                    + "\t\t\t\t<td colspan=\"3\">Total time taken for writing CLOB data</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + ((double) totalClobWriteTime) / 60000 + " minutes</td>\n"
                    + "\t\t\t</tr>\n" + "\t\t\t<tr>\n"
                    + "\t\t\t\t<td colspan=\"3\">Total time taken for writing BLOB data</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + ((double) totalBlobWriteTime) / 60000 + " minutes</td>\n"
                    + "\t\t\t</tr>\n" + "			<tr>\r\n"
                    + "				<td colspan=\"3\">Average time taken for writing CLOB data</td>\r\n"
                    + "				<td colspan=\"3\">" + avgClobTime + " minutes</td>\r\n" + "			</tr>\r\n"
                    + "			<tr>\r\n"
                    + "				<td colspan=\"3\">Average time taken for writing BLOB data</td>\r\n"
                    + "				<td colspan=\"3\">" + avgBlobTime + " minutes</td>\r\n" + "			</tr>\r\n"
                    + "<tr>\n" + "\t\t\t\t<td colspan=\"3\">Total time taken for db data read</td>\n"
                    + "\t\t\t\t<td colspan=\"3\">" + (double) totaldbDataTime / 60000 + " minutes</td>\n"
                    + "\t\t\t</tr>" + "			<tr>\r\n"
                    + "				<td colspan=\"3\">Average time taken for db connection</td>\r\n"
                    + "				<td colspan=\"3\">" + ((double) totalDBConnectionTime / dbHitCounter) / 60000
                    + " minutes</td>\r\n" + "			</tr>\r\n" + "		</table>\r\n"
                    + "	</body>\r\n" + "<br>\r\n" + "<p>Job Reference Id: " + UUID.randomUUID().toString() + "</p>\r\n"
                    + "		<p>Report Generated Date: " + new Date() + "</p>" + "</html>";
            Writer writer = new OutputStreamWriter(new FileOutputStream(psFile, true));
            writer.write(html1);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
