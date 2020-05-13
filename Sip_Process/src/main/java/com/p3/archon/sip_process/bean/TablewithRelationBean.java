package com.p3.archon.sip_process.bean;

import lombok.*;

import java.util.List;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:32 PM.
 */

public class TablewithRelationBean {

    private List<TableDetails> tableList;

    public List<TableDetails> getTableList() {
        return tableList;
    }

    public void setTableList(List<TableDetails> tableList) {
        this.tableList = tableList;
    }
}
