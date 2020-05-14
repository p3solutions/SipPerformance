package com.p3.archon.sip_process.bean;

import com.p3.archon.sip_process.utility.Utility;
import lombok.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.util.List;

/**
 * Created by Suriyanarayanan K
 * on 22/04/20 4:09 PM.
 */
@Builder
@Data
@Getter
@Setter
@ToString
public class InputArgs {

    @Option(name = "-h", aliases = {"--host"}, usage = "Host name for connection", required = true)
    private String host;
    @Option(name = "-p", aliases = {"--port"}, usage = "Port no", required = false)
    private String port;
    @Option(name = "-d", aliases = {"--database"}, usage = "Database name for connection", required = true)
    private String database;
    @Option(name = "-s", aliases = {"--schemaName"}, usage = "Schema name for connection", required = true)
    private String schemaName;
    @Option(name = "-u", aliases = {"--userName"}, usage = "Username for connection", required = true)
    private String user;
    @Option(name = "-pw", aliases = {"--password"}, usage = "Password for connection", required = true)
    private String pass;
    @Option(name = "-ds", aliases = {"--databaseServer"}, usage = "Database server for connection", required = true)
    private String databaseServer;
    @Option(name = "ql", aliases = {"--queryList"}, usage = "Query List", required = false)
    private List<String> queryList;
    @Option(name = "-ol", aliases = {"--output loaction"}, usage = "Ouput files storage location", required = true)
    private String outputLocation;
    @Option(name = "-mt", aliases = {"--main table"}, usage = "Main Table", required = true)
    private String mainTable;
    @Option(name = "-sjf", aliases = {"--json file"}, usage = "Json input file", required = true)
    private String jsonFile;
    @Option(name = "-appName", aliases = {"--Application Name"}, usage = "Application name for Sip Data ", required = true)
    private String appName;
    @Option(name = "-holdName", aliases = {"--Holding Name"}, usage = "Holding Name for Sip Data", required = true)
    private String holdName;
    @Option(name = "-rpx", aliases = {"--rpx"}, usage = "Record Per Xml", required = true)
    private int rpx;
    @Option(name = "-tc", aliases = {"--tc"}, usage = "Thread Count", required = true)
    private int threadCount;
    @Option(name = "-ptc", aliases = {"--ptc"}, usage = "pathThreadCount", required = true)
    private int pathThreadCount;
    @Option(name = "-st", aliases = {"--st"}, usage = "show date time", required = true)
    private boolean showDatetime;


    public void validateInputs() {
        /**
         * Validate RPX And Thread Count
         */
        if (this.rpx < 100) {
            this.rpx = 100 * 1024 * 1024;
        } else {
            this.rpx = this.rpx * 1024 * 1024;
        }
        if (this.threadCount <= 10) {
            this.threadCount = 10;
        }
        if (this.pathThreadCount <= 10) {
            this.pathThreadCount = 10;
        }

        /**
         * Validate Input Json
         */
        JSONObject response = null;
        try {
            response = new JSONObject(Utility.readLineByLine(this.jsonFile));
        } catch (JSONException ex) {
            System.out.println(ex);
            ex.printStackTrace();
            System.exit(1);
        }
        /**
         * Validate Output Location
         */
        File outputLocationDirectory = new File(outputLocation);
        if (!outputLocationDirectory.isDirectory()) {
            System.out.println(outputLocation + "is not a directory");
            System.exit(1);
        }
    }
}
