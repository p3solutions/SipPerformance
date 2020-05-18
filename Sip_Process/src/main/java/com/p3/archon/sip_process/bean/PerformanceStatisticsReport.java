package com.p3.archon.sip_process.bean;

import lombok.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Suriyanarayanan K
 * on 15/05/20 5:10 PM.
 */

@Builder
@Data
@Getter
@Setter
@ToString
public class PerformanceStatisticsReport {

    @Builder.Default
    private long startTime = 0;
    @Builder.Default
    private long totalSipRecord = 0;
    @Builder.Default
    private long totalDatabaseHit = 0;
    @Builder.Default
    private long totalBatchAssemblerTime = 0;
    @Builder.Default
    private long writeSipRecordTIme = 0;
    @Builder.Default
    private Map<Integer, PathPerformance> pathPerformanceMap = new LinkedHashMap<>();
    @Builder.Default
    private long endTime = 0;




}
