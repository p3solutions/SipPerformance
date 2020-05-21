package com.p3.archon.sip_process.bean;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.TreeMap;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:32 PM.
 */

@Getter
@Setter
public class TablewithRelationBean {

    private List<TableDetails> tableList;
    private List<String> selectedTableList;
    public TreeMap<String, String> charReplace;
}
