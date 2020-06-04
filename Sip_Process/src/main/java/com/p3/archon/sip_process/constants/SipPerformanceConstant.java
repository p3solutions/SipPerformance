package com.p3.archon.sip_process.constants;

/**
 * Created by Suriyanarayanan K
 * on 15/05/20 2:22 PM.
 */
public class SipPerformanceConstant {


    public static final String RECORD_FILE = "Record_File";
    public static final String INTERMEDIATE_JSON_FILE = "Intermediate_";
    public static final String ATTACHMENT_FOLDER = "Attachment";
    public static final String FINAL_JSON_FILE = "Final_";
    public static final String JSON = "json";
    public static final String TXT = "txt";

    public static final String NULL_VALUE = "";

    public static final String ALERT_INPUT = "we will assume the default values if User not specify the inputs in a config.properties file . \n" +
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
            "showTime               =   Boolean";


    public static final String DECIMAL_PATTERN = "#.##########################################################################################################################################################################################################################";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String SINGLE_SIP_RECORD_TIME = "SINGLE_SIP_RECORD_TIME";
    public static final String BATCH_ASSEMBLER_TIME = "BATCH_ASSEMBLER_TIME";

    public static final String NEW_LINE = "\n";
    public static final String TAB = "\t";
    public static final String NEW_LINE_TAG = "NL_TAG";
    public static final String TAB_TAG = "TB_TAG";

}
