package com.p3.archon.sip_process;

import com.p3.archon.sip_process.processor.Processor;
import org.apache.log4j.PropertyConfigurator;
import java.util.Date;

/**
 * Created by Suriyanarayanan K
 * on 22/04/20 4:04 PM.
 */
public class SipProcessMain {
    public static void main(String[] args) {

        /**
        * Log 4j properties
         * */
       PropertyConfigurator.configure("log4j.properties");
        System.out.println("Start Time : " + new Date());
        if (args.length == 0) {
            System.err.println("No arguments specified.\nTerminating ... ");
            System.err.println("Job Terminated = " + new Date());
            System.exit(1);
        }

        Processor processor = new Processor(args);
        processor.setValuesIntoBean();
        processor.startExtraction();
        System.out.println("End Time : " + new Date());
    }
}
