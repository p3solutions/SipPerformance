package com.p3.archon.sip_process.bean;

import lombok.*;

import java.util.List;

@Builder
@Data
@Getter
@Setter
@ToString
public class RecordData {
    private String data;
    private List<String> attachmentFiles;
}
