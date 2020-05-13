package com.p3.archon.sip_process.bean;

import java.util.List;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:43 PM.
 */

public class TableDetails {

    private String name;
    private String columnQuery;
    private String filterQuery;
    private List<String> relatedTables;
    private List<String> relationshipList;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColumnQuery() {
        return columnQuery;
    }

    public void setColumnQuery(String columnQuery) {
        this.columnQuery = columnQuery;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public void setFilterQuery(String filterQuery) {
        this.filterQuery = filterQuery;
    }

    public List<String> getRelatedTables() {
        return relatedTables;
    }

    public void setRelatedTables(List<String> relatedTables) {
        this.relatedTables = relatedTables;
    }

    public List<String> getRelationshipList() {
        return relationshipList;
    }

    public void setRelationshipList(List<String> relationshipList) {
        this.relationshipList = relationshipList;
    }
}
