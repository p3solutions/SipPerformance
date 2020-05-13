package com.p3.archon.sip_process.core;

import com.emc.ia.sdk.sip.assembly.*;
import com.emc.ia.sdk.support.io.FileSupplier;
import com.emc.ia.sip.assembly.stringtemplate.StringTemplate;
import com.p3.archon.sip_process.bean.InputArgs;
import com.p3.archon.sip_process.bean.RecordData;

import java.io.File;

public class SipCreator {

    private String appName;
    private String holding;
    private String producer;
    private String namespace;
    private String entity;
    private long rpx;
    protected String outputPath;
    protected BatchSipAssembler<RecordData> batchAssembler;
    private String sipFileName;


    public SipCreator(InputArgs argsBean) {
        this.outputPath = argsBean.getOutputLocation() + File.separator + argsBean.getSchemaName();
        this.rpx = argsBean.getRpx();
        this.appName = argsBean.getAppName();
        this.holding = argsBean.getHoldName();
        producer = "Archon";
        entity = "Archon";
        this.namespace = "urn:x-emc:eas:schema:" + holding + ":1.0";
        this.sipFileName = argsBean.getMainTable();
        create();
    }

    private void create() {

        PackagingInformation prototype = PackagingInformation.builder().dss().application(appName)
                .holding(holding).producer(producer).entity(entity).schema(namespace).end().build();

        PackagingInformationFactory factory = new OneSipPerDssPackagingInformationFactory(
                new DefaultPackagingInformationFactory(prototype), new SequentialDssIdSupplier("ex6dss", 1));

        String sipHeader = "<TABLE_" + sipFileName.toUpperCase() + " xmlns=\"" + namespace + "\">\n";
        String sipFooter = "</TABLE_" + sipFileName.toUpperCase() + ">\n";

        PdiAssembler<RecordData> pdiAssembler = new TemplatePdiAssembler<>(
                new StringTemplate<>(sipHeader, sipFooter, "$model.data$\n"));

        SipAssembler<RecordData> sipAssembler = SipAssembler.forPdiAndContent(factory, pdiAssembler,
                new FilesToDigitalObjects());

        batchAssembler = new BatchSipAssembler<>(sipAssembler, SipSegmentationStrategy.byMaxSipSize(rpx),
                FileSupplier.fromDirectory(new File(outputPath), "sip-" + sipFileName + "-", ".zip"));
    }

    public BatchSipAssembler<RecordData> getBatchAssembler() {
        return batchAssembler;
    }

}
