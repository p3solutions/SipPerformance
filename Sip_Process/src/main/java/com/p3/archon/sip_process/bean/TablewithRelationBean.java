package com.p3.archon.sip_process.bean;

import java.util.List;
import java.util.TreeMap;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:32 PM.
 */

public class TablewithRelationBean {

    private List<TableDetails> tableList;
    public TreeMap<String, String> charReplace = new TreeMap<>();


    public TreeMap<String, String> getCharReplace() {
        return charReplace;
    }

    public void setCharReplace(TreeMap<String, String> charReplace) {
        this.charReplace = charReplace;
    }

    public List<TableDetails> getTableList() {
        return tableList;
    }

    public void setTableList(List<TableDetails> tableList) {
        this.tableList = tableList;
    }
}
