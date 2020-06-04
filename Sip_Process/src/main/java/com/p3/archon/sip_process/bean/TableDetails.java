package com.p3.archon.sip_process.bean;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:43 PM.
 */

@Getter
@Setter
public class TableDetails {

    @Builder.Default
    private String name = "";
    @Builder.Default
    private String modifiedName = "";
    @Builder.Default
    private String columnQuery = "";
    @Builder.Default
    private String blobColumn = "";
    @Builder.Default
    private String filterQuery = "";
    @Builder.Default
    private List<String> keyColumns = new ArrayList<>();
    @Builder.Default
    private List<String> extraColumns = new ArrayList<>();
    @Builder.Default
    private List<String> relatedTables = new ArrayList<>();
    @Builder.Default
    private List<String> relationshipList = new ArrayList<>();
    @Builder.Default
    private TreeMap<String, String> oldModifyColumn = new TreeMap<>();

}
