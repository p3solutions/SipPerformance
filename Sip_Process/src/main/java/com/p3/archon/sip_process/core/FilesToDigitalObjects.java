package com.p3.archon.sip_process.core;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import com.emc.ia.sdk.sip.assembly.DigitalObject;
import com.emc.ia.sdk.sip.assembly.DigitalObjectsExtraction;
import com.p3.archon.sip_process.bean.RecordData;

public class FilesToDigitalObjects implements DigitalObjectsExtraction<RecordData> {
	String attachmentLoc;
	@Override
	public Iterator<? extends DigitalObject> apply(RecordData recordData) {
		return Arrays.stream(getData(recordData)).iterator();
	}

	private DigitalObject[] getData(RecordData recordData) {
		DigitalObject[] digObj = new DigitalObject[recordData.getAttachements().size()];
		int i = 0;
		for (String fileItem : recordData.getAttachements()) {
			File file = new File(fileItem);
			digObj[i++] = (DigitalObject.fromFile(file.getName(), file));
		}
		return digObj;
	}
}
