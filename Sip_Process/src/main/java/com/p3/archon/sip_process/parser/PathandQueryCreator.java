package com.p3.archon.sip_process.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.bean.TableDetails;
import com.p3.archon.sip_process.bean.TablewithRelationBean;
import com.p3.archon.sip_process.utility.Utility;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 4:39 PM.
 */
public class PathandQueryCreator {

    private String jsonFileLocation;
    private String mainTable;
    private TablewithRelationBean tablewithRelationBean;
    private String schemaName;
    private Map<Integer, List<String>> pathTableList = new LinkedHashMap<>();
    private Logger LOGGER = Logger.getLogger(this.getClass().getName());


    public PathandQueryCreator(InputArgs inputBean) {

        this.jsonFileLocation = inputBean.getJsonFile();
        this.mainTable = inputBean.getMainTable();
        this.schemaName = inputBean.getSchemaName();
        createTableBeanAndRequiredCombinationMap();
    }


    /**
     * Responsible for Creation Path List
     */
    public Map<Integer, List<String>> startParsingAndCreationPathList() {
        LOGGER.info("Start Creating Path and Query List");

        return createPathList();
    }

    /**
     * Responsible for Creation Path List
     */
    private Map<Integer, List<String>> createPathList() {
        Set<String> checkedList = new HashSet<>();
        checkedList.add(mainTable);
        int count = 0;
        for (String path : addChildElementIntoTree(mainTable, checkedList)) {
            pathTableList.put(count, Arrays.asList(path.split("->")));
            count++;
        }
        return pathTableList;
    }

    /**
     * Map Input Json FIle into a TablewithRelationBean
     */

    private void createTableBeanAndRequiredCombinationMap() {
        String mainFileLines = Utility.readLineByLine(jsonFileLocation);
        JSONObject sipJson = new JSONObject(mainFileLines);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            tablewithRelationBean = objectMapper.readValue(sipJson.toString(), TablewithRelationBean.class);
        } catch (IOException e) {
            LOGGER.error("Mapping the Json into Table Bean : " + e.getMessage());
        }
    }

    /**
     * Responsible for Creation Query List for all Possible Path
     */
    public Map<Integer, String> getConstructQueryList(Map<Integer, List<String>> pathTableList) {
        Map<Integer, String> queryPathList = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<String>> pathTable : pathTableList.entrySet()) {
            List<String> checkedTableList = new ArrayList<>();
            checkedTableList.addAll(pathTable.getValue());
            String QUERY_STRING = "select " + getColumnQueryString(pathTable.getValue()) +
                    " from " + schemaName + "." + mainTable + " " +
                    String.join("\n", getLeftJoinForAllTable(schemaName + "." + mainTable, checkedTableList)) +
                    (getFilterCondition().isEmpty() ? "" : " where (" + getFilterCondition() + " ) ");
            queryPathList.put(pathTable.getKey(), QUERY_STRING);
        }
        LOGGER.info("Query List Prepared");
        LOGGER.info("Query List Size :" + queryPathList.size());
        return queryPathList;

    }

    /**
     * Responsible for Creation LeftJoin Condition
     */

    private Set<String> getLeftJoinForAllTable(String tableName, List<String> checkedTableList) {

        Set<String> leftJoinQuery = new LinkedHashSet<>();
        List<String> whereQueryList = new ArrayList<>();
        String currentTable = checkedTableList.get(1).split("\\.")[1];
        TableDetails tableDetails = getTableDetailsBasedOnTableName(tableName.split("\\.")[1]);
        tableDetails.getRelationshipList().forEach(
                relationShip -> {
                    if (getRelationFetchStatus(currentTable, relationShip)) {
                        whereQueryList.add(relationShip.split("->")[1]);
                    }
                }
        );
        checkedTableList.remove(tableName);
        leftJoinQuery.add(" LEFT JOIN " + schemaName + "." + currentTable + " ON (" + String.join(" and ", whereQueryList) + " ) ");
        if (checkedTableList.size() > 1) {
            leftJoinQuery.addAll(getLeftJoinForAllTable(checkedTableList.get(0), checkedTableList));
        }
        return leftJoinQuery;
    }

    /**
     * Responsible for Relation Valid Status
     */
    private boolean getRelationFetchStatus(String currentTable, String relationShip) {
        return (relationShip.split("->")[0].equalsIgnoreCase(currentTable));
    }

    /**
     * Responsible for getting All Filter Condition
     */

    private String getFilterCondition() {
        List<String> filterCondition = new ArrayList<>();
        tablewithRelationBean.getTableList()
                .stream().filter(tableDetails -> tableDetails.getFilterQuery() != null && !tableDetails.getFilterQuery().isEmpty()).forEach(tableDetails -> {
            filterCondition.add(tableDetails.getFilterQuery());
        });
        return String.join(" and ", filterCondition);
    }

    /**
     * Responsible for getting All Table Column Query
     */

    private String getColumnQueryString(List<String> tableList) {
        List<String> columnList = new ArrayList<>();
        for (String table : tableList) {
            columnList.add(getTableDetailsBasedOnTableName(table.split("\\.")[1]).getColumnQuery());
        }

        return String.join(",", columnList).equalsIgnoreCase(",,,") ? " * " : String.join(",", columnList);
    }

    /**
     * Responsible for getting Traversal Path List
     */
    private List<String> addChildElementIntoTree(String tableName, Set<String> checkedList) {
        List<String> temporary = getTemporaryTraversalList(tableName, checkedList);
        checkedList.remove(tableName);
        return getTraverseList(tableName, temporary);
    }

    /**
     * Responsible for getting Temporary Traversal Path List
     */

    private List<String> getTemporaryTraversalList(String tableName, Set<String> checkedList) {
        List<String> temporary = new ArrayList<>();
        for (String subChild : getTemporarySubChildList(tableName, checkedList)) {
            if (!checkedList.contains(subChild)) {
                checkedList.add(subChild);
                if (!getTableDetailsBasedOnTableName(tableName).getRelatedTables().isEmpty()) {
                    temporary.addAll(addChildElementIntoTree(subChild, checkedList));
                }
            }
        }
        return temporary;
    }

    /**
     * Responsible for getting Temporary Sub Child List
     */

    private List<String> getTemporarySubChildList(String tableName, Set<String> checkedList) {
        List<String> tempList = new ArrayList<>();
        tempList.addAll(getTableDetailsBasedOnTableName(tableName).getRelatedTables());
        tempList.removeAll(checkedList);
        return tempList;
    }

    /**
     * Responsible for Framing the Traversal List into Path
     */
    private List<String> getTraverseList(String rootTable, List<String> temporary) {
        List<String> output = new ArrayList<>();
        if (temporary.size() == 0) {
            output.add(schemaName + "." + rootTable);
        } else {
            for (String res : temporary) {
                output.add(schemaName + "." + rootTable + "->" + res);
            }
        }
        return output;
    }

    /**
     * Responsible for getting MainTable Filter Query
     */
    public String getMainTableFilterQuery() {
        String filterQuery = getTableDetailsBasedOnTableName(mainTable).getFilterQuery();
        return filterQuery.isEmpty() ? "" : " WHERE (" + filterQuery + " ) ";
    }

    /**
     * Responsible for getting TableDetails Bean
     */
    public TableDetails getTableDetailsBasedOnTableName(String tableName) {
        return tablewithRelationBean.getTableList().stream().filter(tableDetails -> tableDetails.getName().equalsIgnoreCase(tableName)).findFirst().orElse(
                new TableDetails());
    }

    public TreeMap<String, String> getCharacterReplacementMap() {
        return tablewithRelationBean.charReplace;
    }

    public List<String> getBlobQueryList(Integer key, Map<String, List<String>> tablePrimaryKey) {
        List<String> checkedTableList = new ArrayList<>();
        checkedTableList.addAll(prepareTemporarycheckedList(pathTableList.get(key)));
        List<String> blobQueryList = prepareBlobQueryListForAllTable("", mainTable, checkedTableList, tablePrimaryKey, new ArrayList<String>());
        return blobQueryList;
    }

    private List<String> prepareTemporarycheckedList(List<String> pathTableList) {
        List<String> temporaryTableList = new ArrayList<>();
        pathTableList.forEach(table -> temporaryTableList.add(table.split("\\.")[1]));
        return temporaryTableList;
    }

    private List<String> prepareBlobQueryListForAllTable(String parentTable, String childTable, List<String> checkedTableList, Map<String, List<String>> tablePrimaryKey, List<String> parentTableList) {
        List<String> blobQueryPreparedList = new ArrayList<>();
        if (checkedTableList.size() == 0) {
            return blobQueryPreparedList;
        }
        parentTableList.add(childTable);
        checkedTableList.remove(childTable);
        TableDetails tableDetails = getTableDetailsBasedOnTableName(childTable);
        if (parentTable.isEmpty()) {
            if (tableDetails.getBlobColumn() != null && !tableDetails.getBlobColumn().isEmpty()) {
                blobQueryPreparedList.add("SELECT " +
                        Utility.getMainTablePrimaryColumnQuery(tablePrimaryKey.get(childTable))
                        + "," + tableDetails.getBlobColumn() + " FROM " + schemaName + "." + childTable + (tableDetails.getFilterQuery().isEmpty() ? "" : " WHERE ( " + tableDetails.getFilterQuery() + " ) "));
            }
        } else {
            if (tableDetails.getBlobColumn() != null && !tableDetails.getBlobColumn().isEmpty()) {
                blobQueryPreparedList.add("SELECT " + Utility.getMainTablePrimaryColumnQuery(tablePrimaryKey.get(childTable))
                        + "," + tableDetails.getBlobColumn() + " FROM " + getTableNamesLine(parentTableList)
                        + getJoinAndFilterCondition(parentTableList));
            }

        }
        if (checkedTableList.size() != 0) {
            blobQueryPreparedList.addAll(prepareBlobQueryListForAllTable(childTable, checkedTableList.get(0), checkedTableList, tablePrimaryKey, parentTableList));
        }
        return blobQueryPreparedList;
    }

    private String getTableNamesLine(List<String> parentTableList) {
        return parentTableList.stream().map(table -> schemaName + "." + table).collect(Collectors.joining(","));
    }

    private String getJoinAndFilterCondition(List<String> parentTableList) {
        List<String> checkedTableList = new ArrayList<>();
        checkedTableList.addAll(parentTableList);
        StringBuffer joinFilterCondition = new StringBuffer();
        joinFilterCondition.append(" WHERE ( ");
        joinFilterCondition.append(String.join(" AND ", getJoinsConditionsForAllTable(checkedTableList)));
        joinFilterCondition.append(getFilterConditionForParentTableList(parentTableList));
        joinFilterCondition.append(" ) ");
        return joinFilterCondition.toString();
    }

    private String getFilterConditionForParentTableList(List<String> parentTableList) {
        List<String> filterList = new ArrayList<>();
        for (String table : parentTableList) {
            if (!getTableDetailsBasedOnTableName(table).getFilterQuery().isEmpty()) {
                filterList.add(getTableDetailsBasedOnTableName(table).getFilterQuery());
            }
        }
        if (filterList.isEmpty()) {
            return "";
        } else {
            return " AND  ( " + String.join(" AND ", filterList) + " ) ";
        }

    }

    private List<String> getJoinsConditionsForAllTable(List<String> checkedTableList) {
        List<String> joinsCondition = new ArrayList<>();
        String parentTable = checkedTableList.get(0);
        String childTable = checkedTableList.get(1);
        TableDetails parentTableDetails = getTableDetailsBasedOnTableName(parentTable);
        parentTableDetails.getRelationshipList().forEach(
                relationShip -> {
                    if (getRelationFetchStatus(childTable, relationShip)) {
                        joinsCondition.add(relationShip.split("->")[1]);
                    }
                }
        );
        checkedTableList.remove(parentTable);
        if (checkedTableList.size() > 1) {
            joinsCondition.addAll(getJoinsConditionsForAllTable(checkedTableList));
        }
        return joinsCondition;
    }

    public Map<String, List<String>> getPrimaryJoinColumn(List<String> selectedTableList) {
        Map<String, List<String>> tablePrimaryJoinColumn = new LinkedHashMap<>();
        for (String tableName : selectedTableList) {
            TableDetails tableDetails = getTableDetailsBasedOnTableName(tableName);
            tablePrimaryJoinColumn.put(tableDetails.getName(), tableDetails.getKeyColumns());
        }
        return tablePrimaryJoinColumn;
    }

    public List<String> getSelectedTableList() {
        return tablewithRelationBean.getSelectedTableList();
    }

    public String getAllTablesWhereCondition(List<String> selectedTableList) {
        List<String> filterCondition = new ArrayList<>();
        for (String tableName : selectedTableList) {
            TableDetails tableDetails = getTableDetailsBasedOnTableName(tableName);
            if (!tableDetails.getFilterQuery().isEmpty()) {
                filterCondition.add(tableDetails.getFilterQuery());
            }
        }

        return String.join(" AND ", filterCondition);
    }

    public String getSingleTableQuery(String mainTableName) {

        StringBuffer SINGLE_TABLE_QUERY = new StringBuffer();
        SINGLE_TABLE_QUERY.append("SELECT ");
        TableDetails tableDetails = getTableDetailsBasedOnTableName(mainTableName);
        SINGLE_TABLE_QUERY.append(tableDetails.getColumnQuery().isEmpty() ? "*" : tableDetails.getColumnQuery());
        SINGLE_TABLE_QUERY.append(" FROM " + schemaName + "." + mainTableName + " " + (tableDetails.getFilterQuery().isEmpty() ? " " : " WHERE " + tableDetails.getFilterQuery()));
        return SINGLE_TABLE_QUERY.toString();
    }

    public List<String> getSingleTableBlobQueryList(String mainTableName, Map<String, List<String>> tablePrimaryJoinColumn) {
        List<String> blobQueryList = new ArrayList<>();
        TableDetails tableDetails = getTableDetailsBasedOnTableName(mainTableName);
        if (!tableDetails.getBlobColumn().isEmpty()) {
            blobQueryList.add("SELECT " +
                    Utility.getMainTablePrimaryColumnQuery(tablePrimaryJoinColumn.get(mainTableName))
                    + "," + tableDetails.getBlobColumn() + " FROM " + schemaName + "." + mainTableName + (tableDetails.getFilterQuery().isEmpty() ? "" : " WHERE ( " + tableDetails.getFilterQuery() + " ) "));
        }
        return blobQueryList;
    }
}
