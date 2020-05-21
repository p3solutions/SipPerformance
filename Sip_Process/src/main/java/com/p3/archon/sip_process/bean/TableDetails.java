package com.p3.archon.sip_process.bean;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:43 PM.
 */

@Getter
@Setter
public class TableDetails {

    private String name;
    private String columnQuery;
    private String blobColumn;
    private String filterQuery;
    private List<String> keyColumns;
    private List<String> relatedTables;
    private List<String> relationshipList;

}
