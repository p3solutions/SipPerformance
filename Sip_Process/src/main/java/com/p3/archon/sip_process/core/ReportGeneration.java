package com.p3.archon.sip_process.core;

import com.itextpdf.text.Image;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.p3.archon.sip_process.bean.PathReportBean;
import com.p3.archon.sip_process.bean.ReportBean;
import org.apache.commons.io.FileUtils;
import org.xhtmlrenderer.pdf.ITextRenderer;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static com.p3.archon.sip_process.constants.SipPerformanceConstant.DATE_TIME_FORMAT;

/**
 * Created by Suriyanarayanan K
 * on 20/05/20 6:09 PM.
 */
public class ReportGeneration {

    private ReportBean reportBean;

    private long totalClobRecordCounter = 0;
    private long totalBlobRecordCounter = 0;

    private long dbHitCounter = 0;
    private long totalDBConnectionTime = 0;

    private long totalClobRecordWriteTime = 0;
    private long totalBlobRecordWriteTime = 0;

    private long totalResultDataReadTime = 0;
    private long totalBatchAssemblerTime = 0;

    private long totalSourceSipRecord = 0;
    private long totalExtractedSipRecord = 0;
    private long totalSipRecordWriteTime = 0;

    private String outputLocation;

    private static final String LEFT_PADDING = "                                       ";
    public static final String REPORT_OBJECT_PREPARE_STRING = "------------------------------------------------------------------------------------------------------------------------";
    private String JOB_ID = "";
    private long startTime;
    private long endTime;

    private boolean idsFile;
    private boolean sortIdsFile;

    public static final String ARCHON_IMG = "archon.png";

    public ReportGeneration(ReportBean reportBean, String outputLocation, String jobId, boolean idsFile, boolean sortIdsFile) {
        this.reportBean = reportBean;
        this.outputLocation = outputLocation;
        this.JOB_ID = jobId;
        this.idsFile = idsFile;
        this.sortIdsFile = sortIdsFile;
        setValuesIntoReportVariables();
    }

    private void setValuesIntoReportVariables() {
        for (Map.Entry<Integer, PathReportBean> pathPerformanceEntry : reportBean.getPathPerformanceMap().entrySet()) {

            //Path Performance

            dbHitCounter += pathPerformanceEntry.getValue().getDbHitCounter();
            totalDBConnectionTime += pathPerformanceEntry.getValue().getDbConnectionTime();
            totalResultDataReadTime += pathPerformanceEntry.getValue().getResultSetTime();

            totalBlobRecordCounter += pathPerformanceEntry.getValue().getBlobCounter();
            totalClobRecordCounter += pathPerformanceEntry.getValue().getClobCounter();
            totalBlobRecordWriteTime += pathPerformanceEntry.getValue().getBlobTime();
            totalClobRecordWriteTime += pathPerformanceEntry.getValue().getClobTime();

        }

        //Main Table

        dbHitCounter += reportBean.getMainTablePerformance().getDbHitCounter();
        totalDBConnectionTime += reportBean.getMainTablePerformance().getDbConnectionTime();
        totalResultDataReadTime += reportBean.getMainTablePerformance().getResultSetTime();

        totalBlobRecordCounter += reportBean.getMainTablePerformance().getBlobCounter();
        totalClobRecordCounter += reportBean.getMainTablePerformance().getClobCounter();
        totalBlobRecordWriteTime += reportBean.getMainTablePerformance().getBlobTime();
        totalClobRecordWriteTime += reportBean.getMainTablePerformance().getClobTime();


        totalBatchAssemblerTime = reportBean.getTotalBatchAssemblerTime();
        totalSourceSipRecord = reportBean.getTotalSourceSipRecord();
        totalExtractedSipRecord = reportBean.getTotalExtractedSipRecord();
        totalSipRecordWriteTime = reportBean.getWriteSipRecordTIme();

        startTime = reportBean.getStartTime();
        endTime = reportBean.getEndTime();
    }

    public void generatePerformanceReport() {
        try {
            File psFile = new File(outputLocation + File.separator + "Archon_Performance_Statistics" + ".html");
            if (!psFile.exists()) {
                psFile.createNewFile();
            }
            double avgClobTime = totalClobRecordCounter != 0 ? ((double) totalClobRecordWriteTime / totalClobRecordCounter) / 60000 : 0f;
            double avgBlobTime = totalBlobRecordCounter != 0 ? ((double) totalBlobRecordWriteTime / totalBlobRecordCounter) / 60000 : 0f;
            double avgBatchAddTime = totalExtractedSipRecord != 0 ? ((double) totalBatchAssemblerTime / totalExtractedSipRecord) / 60000 : 0f;
            double avgParentRecordCreateTime = totalExtractedSipRecord != 0 ? ((double) totalSipRecordWriteTime / totalExtractedSipRecord) / 60000 : 0f;

            String HTML_STRING = "<html>\n" +
                    "  <head>\n" +
                    "    <h2 align=\"center\">Performance Statistics</h2>\n" +
                    "  </head>\n" +
                    "  <body>\n" +
                    "     <table border=\"2\" style=\"width:100%\" border-spacing=\"5px\">\n" +
                    "         <tr align=\"center\">\n" +
                    "               <td colspan=\"3\">Process</td>\n" +
                    "               <td colspan=\"3\">Values</td>\n" +
                    "         </tr>\n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Total source database hit</td>\n" +
                    "               <td colspan=\"3\">" + dbHitCounter + "</td>\n" +
                    "         </tr>\n" +
                    "         <tr> \n" +
                    "               <td colspan=\"3\">Total number of CLOB data processed</td>\n" +
                    "               <td colspan=\"3\">" + totalClobRecordCounter + "</td>   \n" +
                    "         </tr>\n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Total number of BLOB data processed</td>\n" +
                    "               <td colspan=\"3\">" + totalBlobRecordCounter + "</td>\n" +
                    "         </tr>\n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Average time taken for writing one SIP record</td>\n" +
                    "               <td colspan=\"3\">" + avgParentRecordCreateTime + " minutes</td>\n" +
                    "         </tr> \n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Total time taken for writing SIP records</td>\n" +
                    "               <td colspan=\"3\">" + ((double) totalSipRecordWriteTime) / 60000 + " minutes</td>\n" +
                    "         </tr> \n" +
                    "          <tr>\n" +
                    "               <td colspan=\"3\">Average time taken for adding one record to batch assembler</td>\n" +
                    "               <td colspan=\"3\">" + avgBatchAddTime + " minutes</td>\n" +
                    "         </tr>\n" +
                    "         <tr>    \n" +
                    "               <td colspan=\"3\">Total time taken for adding records to batch assembler</td>\n" +
                    "               <td colspan=\"3\">" + ((double) totalBatchAssemblerTime) / 60000 + "minutes</td>   \n" +
                    "         </tr>\n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Total time taken for writing CLOB data</td>\n" +
                    "               <td colspan=\"3\">" + ((double) totalClobRecordWriteTime) / 60000 + " minutes</td>\n" +
                    "         </tr>\n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Total time taken for writing BLOB data</td>\n" +
                    "               <td colspan=\"3\">" + ((double) totalBlobRecordWriteTime) / 60000 + "  minutes</td>\n" +
                    "         </tr> \n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Average time taken for writing CLOB data</td>\n" +
                    "               <td colspan=\"3\">" + avgClobTime + " minutes</td>\n" +
                    "         </tr> \n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Average time taken for writing BLOB data</td>\n" +
                    "               <td colspan=\"3\">" + avgBlobTime + " minutes</td>\n" +
                    "         </tr>\n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Total time taken for db data read</td>\n" +
                    "               <td colspan=\"3\">" + (double) totalResultDataReadTime / 60000 + " minutes</td>\n" +
                    "         </tr> \n" +
                    "         <tr>\n" +
                    "               <td colspan=\"3\">Average time taken for db connection</td>\n" +
                    "               <td colspan=\"3\">" + ((double) totalDBConnectionTime / dbHitCounter) / 60000 + " minutes</td>\n" +
                    "         </tr>\n" +
                    (idsFile ? "         <tr>\n" +
                            "               <td colspan=\"3\">Ids File Writing Time</td>\n" +
                            "               <td colspan=\"3\">" + ((double) reportBean.getIdsFileWritingTime()) / 60000 + " minutes</td>\n" +
                            "         </tr>\n" : "") +
                    ((idsFile && sortIdsFile) ? "         <tr>\n" +
                            "               <td colspan=\"3\">Ids File Sorting  Time</td>\n" +
                            "               <td colspan=\"3\">" + ((double) reportBean.getIdsFileSortTime()) / 60000 + " minutes</td>\n" +
                            "         </tr>\n" : "") +
                    "     </table> \n" +
                    "  </body>\n" +
                    "  <br> \n" +
                    "  <p>Job Reference Id: " + UUID.randomUUID().toString() + "</p>\r\n" +
                    "  <p>Report Generated Date: " + new Date() + "</p>" +
                    "</html>";
            Writer writer = new OutputStreamWriter(new FileOutputStream(psFile, true));
            writer.write(HTML_STRING);
            writer.flush();
            writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateExtractionAndSummaryReport() {
        StringBuffer extractionBuffer = new StringBuffer();
        StringBuffer summaryBuffer = new StringBuffer();
        viewFormatter(prepareObjectArray(7), true);
        viewFormatter(new Object[]{" JOB", " START TIME", " END TIME", leftPadding("SOURCE RECORDS COUNT", 22),
                leftPadding("DEST RECORDS COUNT", 22), leftPadding("COUNTS MATCH", 15),
                leftPadding("TOTAL PROCESSING TIME", 40)}, false);
        extractionBuffer = createHeader(extractionBuffer, new Object[]{"JOB", "START TIME", "END TIME", "SOURCE RECORDS COUNT",
                "DEST RECORDS COUNT", "COUNTS MATCH", "TOTAL PROCESSING TIME"}, true);
        viewFormatter(prepareObjectArray(7), true);
        extractionBuffer = createData(extractionBuffer,
                new Object[]{JOB_ID, new SimpleDateFormat(DATE_TIME_FORMAT).format(startTime), new SimpleDateFormat(DATE_TIME_FORMAT).format(endTime),
                        totalSourceSipRecord, totalExtractedSipRecord, Boolean.toString(totalSourceSipRecord == totalExtractedSipRecord),
                        timeDiff(endTime - startTime)});
        viewFormatter(new Object[]{JOB_ID, " " + new SimpleDateFormat(DATE_TIME_FORMAT).format(startTime),
                " " + new SimpleDateFormat(DATE_TIME_FORMAT).format(endTime), leftPadding(String.valueOf(totalSourceSipRecord), 22),
                leftPadding(String.valueOf(totalExtractedSipRecord), 22), leftPadding(Boolean.toString(totalSourceSipRecord == totalExtractedSipRecord), 15),
                leftPadding(timeDiff(endTime - startTime), 40)}, false);
        extractionBuffer = createHeader(extractionBuffer);
        summaryBuffer.append(extractionBuffer.toString());
        summaryBuffer = createHeader(summaryBuffer, new Object[]{timeDiff(endTime - startTime)}, false);
        generateReport(summaryBuffer, outputLocation + File.separator + "outSummary.html", "Summary");
        createQueryTable(extractionBuffer);
        if (reportBean.getTableRecordCount().keySet().size() > 1)
            createUniqueTableHeader(extractionBuffer);
        extractionBuffer = createHeader(extractionBuffer, new Object[]{timeDiff(endTime - startTime)}, false);
        generateReport(extractionBuffer, outputLocation + File.separator + "out.html", "Extraction");
        viewFormatter(prepareObjectArray(7), true);

    }

    protected void createReport(String inputFile, String title) {
        try {
            String tempfile = outputLocation + File.separator + title + "_temp.pdf";
            String finalfile = outputLocation + File.separator + title + JOB_ID + ".pdf";
            String url = new File(inputFile).toURI().toURL().toString();
            OutputStream os = new FileOutputStream(new File(tempfile));
            ITextRenderer renderer = new ITextRenderer();
            renderer.setDocument(url);
            renderer.layout();
            renderer.createPDF(os);
            os.close();

            PdfReader pdfReader = new PdfReader(tempfile);
            PdfStamper pdfStamper = new PdfStamper(pdfReader, new FileOutputStream(new File(finalfile)));
            PdfContentByte content = pdfStamper.getUnderContent(pdfReader.getNumberOfPages());
            Image image = Image.getInstance(ARCHON_IMG);
            image.scaleToFit(300f, 300f);
            image.setAbsolutePosition(950f, 20f);
            content.addImage(image);
            pdfStamper.close();
            pdfReader.close();

            FileUtils.forceDeleteOnExit(new File(inputFile));
            FileUtils.forceDeleteOnExit(new File(tempfile));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected StringBuffer createUniqueTableHeader(StringBuffer sb) {
        sb.append("</tbody></table>");
        sb.append("<br></br>");
        sb.append("<br></br>");
        sb.append(
                "<center><b><font style=\"size:14px\">Table Level Details</font></b></center><br></br><table class=\"table table-bordered table-striped\"><thead><tr>");

        sb.append("<td colspan=\"3\" align=\"center\"><b>" + "Table Name" + "</b></td>");
        sb.append("<td colspan=\"2\" align=\"center\"><b>" + "Unique Record Count" + "</b></td>");
        sb.append("</tr></thead><tbody>");

        for (Map.Entry<String, Long> tc : reportBean.getTableRecordCount().entrySet()) {
            sb.append("<tr>");
            sb.append("<td colspan=\"3\" align=\"left\">" + tc.getKey() + "</td>");
            sb.append("<td colspan=\"2\" align=\"right\">" + "N/A" + "</td>");
            sb.append("</tr>");
        }
        return sb;
    }


    protected StringBuffer createQueryTable(StringBuffer sb) {

        String whereCondition = reportBean.getWhereCondition();
        sb.append("</tbody></table>");
        sb.append("<br></br>");
        sb.append("<br></br>");
        sb.append(
                "<center><b><font style=\"size:14px\">Table Filter Condition Details</font></b></center><br></br><table class=\"table table-bordered table-striped\"><tbody>");
        sb.append("<tr>");
        sb.append("<td colspan=\"1\" align=\"left\">" + "Filter Condition" + "</td>");
        sb.append("<td colspan=\"4\" align=\"left\">"
                + (whereCondition.isEmpty() ? "N/A" : whereCondition).replace("<", "&lt;").replace(">", "&gt;")
                + "</td>");
        sb.append("</tr>");
        return sb;
    }

    protected StringBuffer createHeader(StringBuffer sb) {
        StringBuffer sfb = new StringBuffer();
        sfb.append("<html><head><style> @page {  size: 18in 12in; } table {  background-color: transparent;}caption {  padding-top: 8px;  padding-bottom: 8px;  color: #777;  text-align: left;}th {  text-align: left;}.table {  width: 100%;  max-width: 100%;  margin-bottom: 20px;}.table > thead > tr > th,.table > tbody > tr > th,.table > tfoot > tr > th,.table > thead > tr > td,.table > tbody > tr > td,.table > tfoot > tr > td {  padding: 8px;  line-height: 1.42857143;  vertical-align: top;  border-top: 1px solid #ddd;}.table > thead > tr > th {  vertical-align: bottom;  border-bottom: 2px solid #ddd;}.table > caption + thead > tr:first-child > th,.table > colgroup + thead > tr:first-child > th,.table > thead:first-child > tr:first-child > th,.table > caption + thead > tr:first-child > td,.table > colgroup + thead > tr:first-child > td,.table > thead:first-child > tr:first-child > td {  border-top: 0;}.table > tbody + tbody {  border-top: 2px solid #ddd;}.table .table {  background-color: #fff;}.table-condensed > thead > tr > th,.table-condensed > tbody > tr > th,.table-condensed > tfoot > tr > th,.table-condensed > thead > tr > td,.table-condensed > tbody > tr > td,.table-condensed > tfoot > tr > td {  padding: 5px;}.table-bordered {  border: 1px solid #ddd;}.table-bordered > thead > tr > th,.table-bordered > tbody > tr > th,.table-bordered > tfoot > tr > th,.table-bordered > thead > tr > td,.table-bordered > tbody > tr > td,.table-bordered > tfoot > tr > td {  border: 1px solid #ddd;}.table-bordered > thead > tr > th,.table-bordered > thead > tr > td {  border-bottom-width: 2px;}.table-striped > tbody > tr:nth-of-type(odd) {  background-color: #f9f9f9;}.table-hover > tbody > tr:hover {  background-color: #f5f5f5;}table col[class*=\"col-\"] {  position: static;  display: table-column;  float: none;}table td[class*=\"col-\"],table th[class*=\"col-\"] {  position: static;  display: table-cell;  float: none;}.table > thead > tr > td.active,.table > tbody > tr > td.active,.table > tfoot > tr > td.active,.table > thead > tr > th.active,.table > tbody > tr > th.active,.table > tfoot > tr > th.active,.table > thead > tr.active > td,.table > tbody > tr.active > td,.table > tfoot > tr.active > td,.table > thead > tr.active > th,.table > tbody > tr.active > th,.table > tfoot > tr.active > th {  background-color: #f5f5f5;}.table-hover > tbody > tr > td.active:hover,.table-hover > tbody > tr > th.active:hover,.table-hover > tbody > tr.active:hover > td,.table-hover > tbody > tr:hover > .active,.table-hover > tbody > tr.active:hover > th {  background-color: #e8e8e8;}.table > thead > tr > td.success,.table > tbody > tr > td.success,.table > tfoot > tr > td.success,.table > thead > tr > th.success,.table > tbody > tr > th.success,.table > tfoot > tr > th.success,.table > thead > tr.success > td,.table > tbody > tr.success > td,.table > tfoot > tr.success > td,.table > thead > tr.success > th,.table > tbody > tr.success > th,.table > tfoot > tr.success > th {  background-color: #dff0d8;}.table-hover > tbody > tr > td.success:hover,.table-hover > tbody > tr > th.success:hover,.table-hover > tbody > tr.success:hover > td,.table-hover > tbody > tr:hover > .success,.table-hover > tbody > tr.success:hover > th {  background-color: #d0e9c6;}.table > thead > tr > td.info,.table > tbody > tr > td.info,.table > tfoot > tr > td.info,.table > thead > tr > th.info,.table > tbody > tr > th.info,.table > tfoot > tr > th.info,.table > thead > tr.info > td,.table > tbody > tr.info > td,.table > tfoot > tr.info > td,.table > thead > tr.info > th,.table > tbody > tr.info > th,.table > tfoot > tr.info > th {  background-color: #d9edf7;}.table-hover > tbody > tr > td.info:hover,.table-hover > tbody > tr > th.info:hover,.table-hover > tbody > tr.info:hover > td,.table-hover > tbody > tr:hover > .info,.table-hover > tbody > tr.info:hover > th {  background-color: #c4e3f3;}.table > thead > tr > td.warning,.table > tbody > tr > td.warning,.table > tfoot > tr > td.warning,.table > thead > tr > th.warning,.table > tbody > tr > th.warning,.table > tfoot > tr > th.warning,.table > thead > tr.warning > td,.table > tbody > tr.warning > td,.table > tfoot > tr.warning > td,.table > thead > tr.warning > th,.table > tbody > tr.warning > th,.table > tfoot > tr.warning > th {  background-color: #fcf8e3;}.table-hover > tbody > tr > td.warning:hover,.table-hover > tbody > tr > th.warning:hover,.table-hover > tbody > tr.warning:hover > td,.table-hover > tbody > tr:hover > .warning,.table-hover > tbody > tr.warning:hover > th {  background-color: #faf2cc;}.table > thead > tr > td.danger,.table > tbody > tr > td.danger,.table > tfoot > tr > td.danger,.table > thead > tr > th.danger,.table > tbody > tr > th.danger,.table > tfoot > tr > th.danger,.table > thead > tr.danger > td,.table > tbody > tr.danger > td,.table > tfoot > tr.danger > td,.table > thead > tr.danger > th,.table > tbody > tr.danger > th,.table > tfoot > tr.danger > th {  background-color: #f2dede;}.table-hover > tbody > tr > td.danger:hover,.table-hover > tbody > tr > th.danger:hover,.table-hover > tbody > tr.danger:hover > td,.table-hover > tbody > tr:hover > .danger,.table-hover > tbody > tr.danger:hover > th {  background-color: #ebcccc;}.table-responsive {  min-height: .01%;  overflow-x: auto;}@media screen and (max-width: 767px) {  .table-responsive {    width: 100%;    margin-bottom: 15px;    overflow-y: hidden;    -ms-overflow-style: -ms-autohiding-scrollbar;    border: 1px solid #ddd;  }  .table-responsive > .table {    margin-bottom: 0;  }  .table-responsive > .table > thead > tr > th,  .table-responsive > .table > tbody > tr > th,  .table-responsive > .table > tfoot > tr > th,  .table-responsive > .table > thead > tr > td,  .table-responsive > .table > tbody > tr > td,  .table-responsive > .table > tfoot > tr > td {    white-space: nowrap;  }  .table-responsive > .table-bordered {    border: 0;  }  .table-responsive > .table-bordered > thead > tr > th:first-child,  .table-responsive > .table-bordered > tbody > tr > th:first-child,  .table-responsive > .table-bordered > tfoot > tr > th:first-child,  .table-responsive > .table-bordered > thead > tr > td:first-child,  .table-responsive > .table-bordered > tbody > tr > td:first-child,  .table-responsive > .table-bordered > tfoot > tr > td:first-child {    border-left: 0;  }  .table-responsive > .table-bordered > thead > tr > th:last-child,  .table-responsive > .table-bordered > tbody > tr > th:last-child,  .table-responsive > .table-bordered > tfoot > tr > th:last-child,  .table-responsive > .table-bordered > thead > tr > td:last-child,  .table-responsive > .table-bordered > tbody > tr > td:last-child,  .table-responsive > .table-bordered > tfoot > tr > td:last-child {    border-right: 0;  }  .table-responsive > .table-bordered > tbody > tr:last-child > th,  .table-responsive > .table-bordered > tfoot > tr:last-child > th,  .table-responsive > .table-bordered > tbody > tr:last-child > td,  .table-responsive > .table-bordered > tfoot > tr:last-child > td {    border-bottom: 0;  }}</style></head><body><center><b><font style=\"size:14px\">Archon Extraction Report</font></b></center><br></br><table class=\"table table-bordered table-striped\"><thead><tr>");
        sfb.append(sb.toString());
        return sfb;
    }

    protected void generateReport(StringBuffer sb, String inputFile, String title) {
        try {
            Writer writer = new OutputStreamWriter(new FileOutputStream(inputFile, true));
            writer.write(sb.toString());
            writer.flush();
            writer.close();

            createReport(inputFile, title);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected StringBuffer createHeader(StringBuffer sb, Object[] strings, boolean flag) {
        if (flag) {
            sb.append("<td colspan=\"3\" align=\"center\"><b>" + strings[0] + "</b></td>");
            sb.append("<td colspan=\"2\" align=\"center\"><b>" + strings[1] + "</b></td>");
            sb.append("<td colspan=\"2\" align=\"center\"><b>" + strings[2] + "</b></td>");
            sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[3] + "</b></td>");
            sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[4] + "</b></td>");
            sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[5] + "</b></td>");
            sb.append("<td colspan=\"2\" align=\"center\"><b>" + strings[6] + "</b></td>");
            sb.append("</tr></thead><tbody>");
        } else {
            sb.append("</tbody></table>");
            sb.append("<br></br>");
            sb.append("<br></br>");
            sb.append("<br></br>");
            sb.append("<br></br>");
            sb.append("<p>Job Reference Id : " + JOB_ID + "</p>");
            sb.append("<p>Generated with Archon</p>");
            sb.append("<p>Generated Date : " + new Date() + "</p>");
            sb.append("<p>Job Execution Time : " + strings[0] + "</p>");
            sb.append("<p><i><font size=\"8px\">This is a system generated report.</font></i></p>");
            sb.append("</body></html>");
        }
        return sb;
    }

    protected StringBuffer createData(StringBuffer sb, Object[] strings) {
        sb.append("<tr>");
        sb.append("<td colspan=\"3\" align=\"left\">" + strings[0] + "</td>");
        sb.append("<td colspan=\"2\" align=\"center\">" + strings[1] + "</td>");
        sb.append("<td colspan=\"2\" align=\"center\">" + strings[2] + "</td>");
        sb.append("<td colspan=\"1\" align=\"right\">" + strings[3] + "</td>");
        sb.append("<td colspan=\"1\" align=\"right\">" + strings[4] + "</td>");

        if (strings[5].toString().equalsIgnoreCase("true"))
            sb.append("<td colspan=\"1\" align=\"center\"><b><font color=\"green\">YES</font></b></td>");
        else
            sb.append("<td colspan=\"1\" align=\"center\"><b><font color=\"red\">NO</font></b></td>");
        sb.append("<td colspan=\"2\" align=\"right\">" + strings[6] + "</td>");
        sb.append("</tr>");
        return sb;
    }

    public static String leftPadding(String str, int num) {
        return LEFT_PADDING.substring(0, (num - str.length())) + str;
    }

    protected static Object[] prepareObjectArray(int i) {
        Object[] array = new Object[i];
        for (int j = 0; j < array.length; j++) {
            array[j] = REPORT_OBJECT_PREPARE_STRING.substring(0, lengthArray[j]);
        }
        return array;
    }

    public static String rightPadding(String str, int num) {
        return String.format("%1$-" + num + "s", str);
    }

    static int[] lengthArray = new int[]{40, 30, 30, 25, 25, 18, 43};

    protected static void viewFormatter(Object[] strings, boolean flag) {
        System.out.print(flag ? "+" : "|");
        for (int i = 0; i < strings.length; i++) {
            System.out.print(rightPadding(strings[i] + "", lengthArray[i]));
            System.out.print(flag ? "+" : "|");
        }
        System.out.println();
    }

    public static String timeDiff(long diff) {
        int diffDays = (int) (diff / (24 * 60 * 60 * 1000));
        String dateFormat = "";
        if (diffDays > 0) {
            dateFormat += diffDays + " day ";
        }
        diff -= diffDays * (24 * 60 * 60 * 1000);

        int diffhours = (int) (diff / (60 * 60 * 1000));
        if (diffhours > 0) {
            dateFormat += leftNumPadding(diffhours, 2) + " hour ";
        } else if (dateFormat.length() > 0) {
            dateFormat += "00 hour ";
        }
        diff -= diffhours * (60 * 60 * 1000);

        int diffmin = (int) (diff / (60 * 1000));
        if (diffmin > 0) {
            dateFormat += leftNumPadding(diffmin, 2) + " min ";
        } else if (dateFormat.length() > 0) {
            dateFormat += "00 min ";
        }

        diff -= diffmin * (60 * 1000);

        int diffsec = (int) (diff / (1000));
        if (diffsec > 0) {
            dateFormat += leftNumPadding(diffsec, 2) + " sec ";
        } else if (dateFormat.length() > 0) {
            dateFormat += "00 sec ";
        }

        int diffmsec = (int) (diff % (1000));
        dateFormat += leftNumPadding(diffmsec, 3) + " ms";
        return dateFormat;
    }

    private static String leftNumPadding(int str, int num) {
        return String.format("%0" + num + "d", str);
    }

}
