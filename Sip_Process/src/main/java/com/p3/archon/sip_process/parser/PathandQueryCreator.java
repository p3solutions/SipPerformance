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

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 4:39 PM.
 */
public class PathandQueryCreator {

    private String jsonFileLocation;
    private String mainTable;
    private TablewithRelationBean tablewithRelationBean;
    private String schemaName;

    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public PathandQueryCreator(InputArgs inputBean) {
        this.jsonFileLocation = inputBean.getJsonFile();
        this.mainTable = inputBean.getMainTable();
        this.schemaName = inputBean.getSchemaName();
    }


    /**
     * Responsible for Creation Path List
     */
    public Map<Integer, List<String>> startParsingAndCreationPathList() {
        LOGGER.info("Start Creating Path and Query List");
        createTableBeanAndRequiredCombinationMap();
        return createPathList();
    }

    /**
     * Responsible for Creation Path List
     */
    private Map<Integer, List<String>> createPathList() {
        Set<String> checkedList = new HashSet<>();
        checkedList.add(mainTable);
        Map<Integer, List<String>> pathTableList = new LinkedHashMap<>();
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
        return tablewithRelationBean.getTableList().stream().filter(tableDetails -> tableDetails.getName().equalsIgnoreCase(tableName)).findFirst().orElse(new TableDetails());
    }

    public TreeMap<String, String> getCharacterReplacementMap() {
        return tablewithRelationBean.charReplace;
    }
}
