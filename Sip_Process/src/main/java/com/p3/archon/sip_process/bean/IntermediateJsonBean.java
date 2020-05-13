package com.p3.archon.sip_process.bean;

import lombok.*;

import java.util.List;
import java.util.Map;

/**
 * Created by Suriyanarayanan K
 * on 08/05/20 3:17 PM.
 */
@Builder
@Data
@Getter
@Setter
@ToString
public class IntermediateJsonBean {
    String testFileLocation;
    Map<String, Long> tableColumnCount;
    List<String> headerList;
    Map<String, List<Integer>> tablePrimaryHeaderPosition;
}
