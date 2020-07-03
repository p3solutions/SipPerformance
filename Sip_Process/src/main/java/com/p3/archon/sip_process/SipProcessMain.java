package com.p3.archon.sip_process;

import com.p3.archon.sip_process.processor.Processor;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Date;

/**
 * Created by Suriyanarayanan K
 * on 22/04/20 4:04 PM.
 */
public class SipProcessMain {


    public static void main(String[] args) {
        Logger LOGGER = Logger.getLogger(SipProcessMain.class);

        LOGGER.info("Start Time : " + new Date());
        /**
         *
         * Log 4j properties
         *
         * */
        PropertyConfigurator.configure("log4j.properties");
        Processor processor = new Processor();
        processor.setValuesIntoBean();
        processor.startExtraction();
        LOGGER.info("End Time : " + new Date());
    }
}
