package com.p3.archon.sip_process.bean;

import lombok.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Suriyanarayanan K
 * on 15/05/20 5:10 PM.
 */

@Builder
@Data
@Getter
@Setter
@ToString
public class ReportBean {

    @Builder.Default
    private long startTime = 0;
    @Builder.Default
    private long totalSourceSipRecord = 0;
    @Builder.Default
    private long totalExtractedSipRecord = 0;
    @Builder.Default
    private long totalBatchAssemblerTime = 0;
    @Builder.Default
    private long writeSipRecordTIme = 0;
    @Builder.Default
    private Map<Integer, PathReportBean> pathPerformanceMap = new LinkedHashMap<>();
    @Builder.Default
    private PathReportBean mainTablePerformance = PathReportBean.builder().build();
    @Builder.Default
    private Map<String, Long> tableRecordCount = new LinkedHashMap<>();
    @Builder.Default
    private long endTime = 0;
    @Builder.Default
    private String whereCondition = "";


}
