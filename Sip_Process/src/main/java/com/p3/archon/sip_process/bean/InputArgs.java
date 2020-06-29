package com.p3.archon.sip_process.bean;

import com.p3.archon.sip_process.utility.Utility;
import lombok.*;
import org.json.JSONException;
import org.json.JSONObject;

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


    private String hostName;
    private String portNo;
    private String databaseName;
    private String schemaName;
    private String user;
    private String pass;
    private String databaseServer;
    private List<String> queryList;
    private String outputLocation;
    private String mainTable;
    private String jsonFile;
    private String appName;
    private String holdName;
    private int rpx;
    private int threadCount;
    private int pathThreadCount;
    private boolean showDatetime;
    private String jobId;

    //  Record File Process
    private boolean isRecordFileProcess;
    private List<String> fileNameList;
    private String mainTablePrimaryColumnFileName;
    private String columnDataType;

    //  Clean Operation
    private boolean isCleanUpData;

    // isIdfFile

    private boolean isIdsFile;
    private boolean sortIdsFile;

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
