package com.p3.archon.sip_process.bean;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

/**
 * Created by Suriyanarayanan K
 * on 05/05/20 6:32 PM.
 */

@Getter
@Setter
public class TablewithRelationBean {

    @Builder.Default
    private List<TableDetails> tableList=new ArrayList<>();
    @Builder.Default
    private List<String> selectedTableList=new ArrayList<>();
    @Builder.Default
    public TreeMap<String, String> charReplace=new TreeMap<>();
}
