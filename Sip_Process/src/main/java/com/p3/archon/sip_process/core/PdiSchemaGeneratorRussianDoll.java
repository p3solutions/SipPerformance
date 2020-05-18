package com.p3.archon.sip_process.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.bean.TablewithRelationBean;
import com.p3.archon.sip_process.utility.Utility;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.*;
import java.net.URI;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Suriyanarayanan K
 * on 07/05/20 6:43 PM.
 */
public class PdiSchemaGeneratorRussianDoll {

    private Logger LOGGER = Logger.getLogger(this.getClass().getName());
    private Writer pdiSchemaWriter;
    private static final String PDI_SCHEMA_FILE = "pdi-schema.xsd";
    private static final String ENCODING = "UTF-8";
    private String nameSpace;
    private int i = 0;
    private String tabSpace = "\n";
    private int level = 0;
    private String mainTable;
    private TreeSet<String> checkList = new TreeSet<String>();
    private TablewithRelationBean tablewithRelationBean;
    private Map<String, Integer> columnDataTypeMap;
    private InputArgs argsBean;
    private boolean showDateTime;

    public PdiSchemaGeneratorRussianDoll(InputArgs argsBean, Map<String, Integer> columnDataTypeMap) throws IOException {
        this.columnDataTypeMap = columnDataTypeMap;
        this.mainTable = argsBean.getMainTable();
        this.nameSpace = generateNameSpace(argsBean.getHoldName());
        this.argsBean = argsBean;
        this.showDateTime = argsBean.isShowDatetime();
    }


    public boolean startPdiSchemaCreation() {
        boolean status = true;
        String mainFileLines = Utility.readLineByLine(argsBean.getJsonFile());
        JSONObject sipJson = new JSONObject(mainFileLines);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            tablewithRelationBean = objectMapper.readValue(sipJson.toString(), TablewithRelationBean.class);
            LOGGER.info("The Schema Type -> Russian Doll");
            File path = new File(argsBean.getOutputLocation());
            if (!path.exists())
                path.mkdirs();
            pdiSchemaWriter = new OutputStreamWriter(new FileOutputStream(argsBean.getOutputLocation() + File.separator + PDI_SCHEMA_FILE),
                    ENCODING);
            pdiSchemaWriter.write("<?xml version=\"1.0\" encoding=\"");
            pdiSchemaWriter.write(ENCODING);
            pdiSchemaWriter.write("\"?>");
            pdiSchemaWriter.write(getTabSpace(level++)
                    + "<xs:schema attributeFormDefault=\"unqualified\" elementFormDefault=\"qualified\" ");
            pdiSchemaWriter.write("targetNamespace=\"");
            pdiSchemaWriter.write(nameSpace + "\" ");
            pdiSchemaWriter.write("xmlns=\"");
            pdiSchemaWriter.write(nameSpace + "\" ");
            pdiSchemaWriter.write("xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"");
            pdiSchemaWriter.write(">");
            createSchema(mainTable);
            pdiSchemaWriter.write("\n</xs:schema>");
            pdiSchemaWriter.flush();
            pdiSchemaWriter.close();
            LOGGER.info("pdi schema file created at " + argsBean.getOutputLocation() + File.separator + PDI_SCHEMA_FILE + "\n");
        } catch (IOException e) {
            status = false;
            LOGGER.error("Pdi Creation Job was Failed :  " + e.getMessage());
        }
        return status;
    }


    public String generateNameSpace(String holding) {
        return URI.create("urn:x-emc:ia:schema:" + holding + ":1.0").toString();
    }

    private void createSchema(String tableName) throws IOException {
        if (checkList.contains(tableName))
            return;
        checkList.add(tableName);
        if (tableName.equalsIgnoreCase(mainTable))
            pdiSchemaWriter.write(getTabSpace(level++) + "<xs:element name=\"TABLE_" + tableName.toUpperCase() + "\">");
        else
            pdiSchemaWriter.write(getTabSpace(level++) + "<xs:element maxOccurs=\"1\" minOccurs=\"0\" name=\"TABLE_" + tableName.toUpperCase()
                    + "\">");
        pdiSchemaWriter.write(getTabSpace(level++) + "<xs:complexType>");
        pdiSchemaWriter.write(getTabSpace(level++) + "<xs:sequence>");
        pdiSchemaWriter.write(getTabSpace(level++) + "<xs:element maxOccurs=\"unbounded\" minOccurs=\"0\" name=\"" + tableName.toUpperCase()
                + "_ROW\">");
        pdiSchemaWriter.write(getTabSpace(level++) + "<xs:complexType>");
        pdiSchemaWriter.write(getTabSpace(level++) + "<xs:sequence>");
        getColumns(tableName);
        List<String> relationShipTable = tablewithRelationBean.getTableList().stream().filter(tableDetails -> tableDetails.getName().equalsIgnoreCase(tableName)).flatMap(tableDetails -> tableDetails.getRelatedTables().stream()).collect(Collectors.toList());
        Collections.sort(relationShipTable);
        for (String childTable : relationShipTable) {
            if (checkList.contains(childTable))
                continue;
            createSchema(childTable);
        }

        pdiSchemaWriter.write(getTabSpace(level--) + "</xs:sequence>");
        pdiSchemaWriter.write(getTabSpace(level--) + "</xs:complexType>");
        pdiSchemaWriter.write(getTabSpace(level--) + "</xs:element>");
        pdiSchemaWriter.write(getTabSpace(level--) + "</xs:sequence>");
        pdiSchemaWriter.write(getTabSpace(level--) + "</xs:complexType>");
        pdiSchemaWriter.write(getTabSpace(level--) + "</xs:element>");
        checkList.remove(tableName);
    }

    private void getColumns(String tableName) throws IOException {

        List<String> originalColumnNameList = new ArrayList<>();
        List<String> temporaryColumnList = tablewithRelationBean.getTableList().stream().filter(tableDetails -> tableDetails.getName().equalsIgnoreCase(tableName)).flatMap(tableDetails -> Arrays.asList(tableDetails.getColumnQuery().split(",")).stream()).collect(Collectors.toList());

        for (String temporaryColumn : temporaryColumnList) {
            originalColumnNameList.add(temporaryColumn.split(" as ")[0].trim());
        }
        Collections.sort(originalColumnNameList);
        for (String column : originalColumnNameList) {
            if (isDateKindDataType(column)) {
                column = column.split("\\.")[1];
                pdiSchemaWriter.write(getTabSpace(level) + "<xs:element name=\"" + column.toUpperCase()
                        + "\" type=\"xs:string\" minOccurs=\"0\"/>");
                pdiSchemaWriter.write(getTabSpace(level) + "<xs:element name=\"" + column.toUpperCase()
                        + "_DT_COMPATIBLE\" minOccurs=\"0\">");
                level++;
                pdiSchemaWriter.write(getTabSpace(level++) + "<xs:complexType>");
                pdiSchemaWriter.write(getTabSpace(level++) + "<xs:simpleContent>");
                pdiSchemaWriter.write(getTabSpace(level++) + "<xs:extension base=\"xs:dateTime\">");
                pdiSchemaWriter.write(getTabSpace(level++)
                        + "<xs:attribute name=\"createdBy\" type=\"xs:string\" use=\"optional\"/>");
                level -= 2;
                pdiSchemaWriter.write(getTabSpace(level--) + "</xs:extension>");
                pdiSchemaWriter.write(getTabSpace(level--) + "</xs:simpleContent>");
                pdiSchemaWriter.write(getTabSpace(level--) + "</xs:complexType>");
                pdiSchemaWriter.write(getTabSpace(level) + "</xs:element>");
            } else {
                pdiSchemaWriter.write(getTabSpace(level) + "<xs:element name=\"" + column.split("\\.")[1].toUpperCase() + "\" type=\""
                        + getColumnType((columnDataTypeMap.get(column))) + "\" minOccurs=\"0\"/>");
            }
        }
        level++;
    }

    private String getColumnType(Integer colType) {
        switch (colType) {
            case Types.CHAR:
            case Types.VARCHAR:
           /* case Types.TEXT":
            case Types.TINYTEXT":
            case Types.MEDIUMTEXT":
            case Types.LONGTEXT":
            case Types.USERDEFINED":*/
                return "xs:string";
            case Types.INTEGER:
           /* case Types.INT":
            case Types.AUTONUMBER":*/
            case Types.NUMERIC:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.TINYINT:
                return "xs:int";
            // case Types.LONG":
            case Types.LONGVARCHAR:
                return "xs:long";
            case Types.DOUBLE:
            case Types.DECIMAL:
           /* case Types.MONEY":
            case Types.DEC":
            case Types.SMALLMONEY":
            case Types.BIGMONEY":
            case Types.CURRENCY":*/
                return "xs:double";
            case Types.FLOAT:
            case Types.REAL:
                return "xs:float";
            case Types.DATE:
                return "xs:date";
            case Types.TIME:
                return "xs:time";
            //case Types.DATETIME":
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                /*case Types.TIMESTAMP WITH LOCAL TIME ZONE:
                 case Types.SMALLDATETIME":*/
                return "xs:string";
            default:
                return "xs:string";
        }
    }

    private boolean isDateKindDataType(String column) {
        int type = columnDataTypeMap.get(column);
        if ((type == Types.DATE && showDateTime) || type == Types.TIME_WITH_TIMEZONE || type == Types.TIMESTAMP_WITH_TIMEZONE ||
                type == Types.TIMESTAMP) {
            return true;
        } else {
            return false;
        }
    }

    private String getTabSpace(int tabSize) {
        if (tabSize < 0)
            return "";
        i = 0;
        tabSpace = "\n";
        while (i++ != tabSize) {
            tabSpace += "\t";
        }
        return tabSpace;
    }
}
