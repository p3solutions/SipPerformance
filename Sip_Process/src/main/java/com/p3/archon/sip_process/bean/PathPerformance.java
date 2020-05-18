package com.p3.archon.sip_process.bean;

import lombok.*;

/**
 * Created by Suriyanarayanan K
 * on 15/05/20 5:12 PM.
 */

@Builder
@Data
@Getter
@Setter
@ToString

public class PathPerformance {

    @Builder.Default
    private long queryTime = 0;
    @Builder.Default
    private long normalCounter = 0;
    @Builder.Default
    private long clobCounter = 0;
    @Builder.Default
    private long blobCounter = 0;
    @Builder.Default
    private long normalTime = 0;
    @Builder.Default
    private long clobTime = 0;
    @Builder.Default
    private long blobTime = 0;
    @Builder.Default
    private long resultTime = 0;

}
