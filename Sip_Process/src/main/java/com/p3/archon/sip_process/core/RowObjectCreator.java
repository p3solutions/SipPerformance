package com.p3.archon.sip_process.core;

import com.p3.archon.sip_process.bean.BinaryData;
import com.p3.archon.sip_process.utility.Utility;
import org.apache.xmlbeans.impl.common.XMLChar;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import org.apache.log4j.Logger;

import static com.p3.archon.sip_process.constants.SipPerformanceConstant.*;

/**
 * Created by Suriyanarayanan K
 * on 14/05/20 2:23 PM.
 */
public class RowObjectCreator {


    private boolean showLobs = true;
    private boolean isShowDateTime;

    private long stringRecordCounter = 0;
    private long clobCounter = 0;
    private long blobCounter = 0;

    private long totalStringRecordWriteTime = 0;
    private long totalBlobWriteTime = 0;
    private long totalClobWriteTime = 0;


    private Map<String, List<String>> tablePrimaryKeyColumns = new LinkedHashMap<>();
    private ResultSetMetaData resultSetMetaData;
    private TreeMap<String, String> charReplacement;

    private DecimalFormat formatter;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
    private final SimpleDateFormat dateOnlyFormat = new SimpleDateFormat(DATE_FORMAT);

    private String attachmentFolderName;
    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public RowObjectCreator(ResultSetMetaData resultSetMetaData, String attachmentLocation, TreeMap<String, String> charReplacement, boolean isShowDateTime) {
        this.resultSetMetaData = resultSetMetaData;
        this.attachmentFolderName = attachmentLocation + File.separator + ATTACHMENT_FOLDER;
        new File(attachmentFolderName).mkdirs();
        this.charReplacement = charReplacement;
        this.isShowDateTime = isShowDateTime;
    }

    public void generateBlobFiles(ResultSet rs) throws SQLException {
        List fileName = new ArrayList();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            int type = resultSetMetaData.getColumnType(i);
            long startTimeRec;
            Object columnData;
            final Blob blob;
            switch (type) {
                case Types.BLOB:
                    startTimeRec = System.currentTimeMillis();
                    blob = rs.getBlob(i);
                    if (rs.wasNull() || blob == null) {
                        columnData = null;
                    } else {
                        blobCounter++;
                        columnData = readBlob(blob);
                    }
                    getBlobInfo(columnData, columnName, String.join("_", fileName));
                    totalBlobWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    startTimeRec = System.currentTimeMillis();
                    final InputStream stream = rs.getBinaryStream(i);
                    blob = rs.getBlob(i);
                    if (rs.wasNull() || stream == null) {
                        columnData = new String();
                    } else {
                        blobCounter++;
                        columnData = readStream(stream, blob);
                    }
                    getBlobInfo(columnData, columnName, String.join("__", fileName));
                    totalBlobWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                default:
                    fileName.add(getStringFromObjectValue(rs, i));
                    break;
            }
        }

    }

    private void getBlobInfo(Object columnData, String columnName, String primaryKeyValues) {
        if (columnData != null) {
            try {
                String validFileName = getValidNameForThatFile(columnName, primaryKeyValues);
                LOGGER.debug("File Name :" + validFileName);
                String fileName = attachmentFolderName + File.separator + validFileName;
                if (!new File(fileName).exists()) {
                    BinaryData data = ((BinaryData) columnData);
                    InputStream in = data.getBlob().getBinaryStream();
                    OutputStream out = new FileOutputStream(fileName);
                    byte[] buff = new byte[1024];
                    int len = 0;
                    while ((len = in.read(buff)) != -1) {
                        out.write(buff, 0, len);
                    }
                    out.flush();
                    out.close();
                    in.close();
                }
            } catch (Exception e) {
                LOGGER.debug("Exception in Blob Creation :" + e.getStackTrace());
            }
        }
    }

    private String getValidNameForThatFile(String columnName, String primaryKeyValues) {
        return Utility.getTextFormatted(columnName.trim().replace(".", "__") + "__" + primaryKeyValues);
    }

    public List<String> getRowObjectList(ResultSet rs) throws SQLException {
        Map<String, String> keyPairRecordValues = new LinkedHashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            int type = resultSetMetaData.getColumnType(i);
            Object columnData;
            long startTimeRec;
            final Blob blob;
            switch (type) {
                case Types.CLOB:
                    startTimeRec = System.currentTimeMillis();
                    final Clob clob = rs.getClob(i);
                    if (rs.wasNull() || clob == null) {
                        columnData = null;
                    } else {
                        clobCounter++;
                        columnData = readClob(clob, null);
                    }
                    keyPairRecordValues.put(columnName.trim(), getCdataString(columnData));
                    totalClobWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.NCLOB:
                    startTimeRec = System.currentTimeMillis();
                    final NClob nClob = rs.getNClob(i);
                    if (rs.wasNull() || nClob == null) {
                        columnData = null;
                    } else {
                        clobCounter++;
                        columnData = readClob(nClob, null);
                    }
                    keyPairRecordValues.put(columnName.trim(), (getCdataString(columnData)));
                    totalClobWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.BLOB:
                    startTimeRec = System.currentTimeMillis();
                    blob = rs.getBlob(i);
                    if (rs.wasNull() || blob == null) {
                        columnData = null;
                    } else {
                        blobCounter++;
                        columnData = readBlob(blob);
                    }
                    keyPairRecordValues.put(columnName.trim(), getBlobInfo(columnData, columnName, keyPairRecordValues));
                    totalBlobWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    startTimeRec = System.currentTimeMillis();
                    final InputStream stream = rs.getBinaryStream(i);
                    blob = rs.getBlob(i);
                    if (rs.wasNull() || stream == null) {
                        columnData = new String();
                    } else {
                        blobCounter++;
                        columnData = readStream(stream, blob);
                    }
                    keyPairRecordValues.put(columnName.trim(), getBlobInfo(columnData, columnName, keyPairRecordValues));
                    totalBlobWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.DATE:
                    startTimeRec = System.currentTimeMillis();
                    final Date datevalue = rs.getDate(i);
                    if (rs.wasNull() || datevalue == null) {
                        columnData = new String();
                    } else {
                        stringRecordCounter++;
                        try {
                            java.sql.Date ts = rs.getDate(i);
                            Date date = new Date();
                            date.setTime(ts.getTime());
                            String formattedDate;
                            if (isShowDateTime) {
                                formattedDate = dateFormat.format(date);
                            } else
                                formattedDate = dateOnlyFormat.format(date);
                            columnData = formattedDate;
                        } catch (Exception e) {
                            columnData = rs.getDate(i);
                        }
                    }
                    if (isShowDateTime) {
                        keyPairRecordValues.put(columnName.trim(), getDateValue(columnData.toString()));
                    } else
                        keyPairRecordValues.put(columnName.trim(), getXmlValidStringData(columnData.toString(), false));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.TIMESTAMP:
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    startTimeRec = System.currentTimeMillis();
                    final Timestamp timestampValue = rs.getTimestamp(i);
                    if (rs.wasNull() || timestampValue == null) {
                        keyPairRecordValues.put(columnName.trim(), NULL_VALUE);
                        totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    } else {
                        stringRecordCounter++;
                        try {
                            Timestamp ts = rs.getTimestamp(i);
                            Date date = new Date();
                            date.setTime(ts.getTime());
                            String formattedDate = dateFormat.format(date);
                            columnData = formattedDate;
                        } catch (Exception e) {
                            columnData = rs.getTimestamp(i);
                        }
                        keyPairRecordValues.put(columnName.trim(), getDateValue(columnData.toString()));
                        totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    }

                    break;
                case Types.BIT:
                    startTimeRec = System.currentTimeMillis();
                    final boolean booleanValue = rs.getBoolean(i);
                    final String stringValue = rs.getString(i);
                    if (rs.wasNull()) {
                        columnData = new String();
                    } else {
                        stringRecordCounter++;
                        columnData = stringValue.equalsIgnoreCase(Boolean.toString(booleanValue)) ? booleanValue
                                : stringValue;
                    }
                    keyPairRecordValues.put(columnName.trim(), getXmlValidStringData(columnData.toString(), false));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.FLOAT:
                    startTimeRec = System.currentTimeMillis();
                    final float floatValue = rs.getFloat(i);
                    if (rs.wasNull()) {
                        columnData = new String();
                    } else {
                        stringRecordCounter++;
                        float value = (float) floatValue;
                        if (Math.abs(value - (int) value) > 0.0)
                            formatter = new DecimalFormat(
                                    DECIMAL_PATTERN);
                        else
                            formatter = new DecimalFormat("#");
                        columnData = formatter.format(value);
                    }
                    keyPairRecordValues.put(columnName.trim(), getXmlValidStringData(columnData.toString(), false));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.DOUBLE:
                    startTimeRec = System.currentTimeMillis();
                    final double doubleValue = rs.getDouble(i);
                    if (rs.wasNull()) {
                        columnData = new String();
                    } else {
                        stringRecordCounter++;
                        double value = (double) doubleValue;
                        if (Math.abs(value - (int) value) > 0.0)
                            formatter = new DecimalFormat(DECIMAL_PATTERN);
                        else
                            formatter = new DecimalFormat("#");
                        columnData = formatter.format(value);
                    }
                    keyPairRecordValues.put(columnName.trim(), getXmlValidStringData(columnData.toString(), false));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                default:
                    startTimeRec = System.currentTimeMillis();
                    keyPairRecordValues.put(columnName.trim(), getStringFromObjectValue(rs, i));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
            }
        }
        return new ArrayList<>(keyPairRecordValues.values());
    }

    private String getBlobInfo(Object columnData, String columnName, Map<String, String> keyPairRecordValues) {
        List fileNameSuffixList = new ArrayList();
        if (columnData == null) {
            return NULL_VALUE;
        } else {
            if (tablePrimaryKeyColumns != null && tablePrimaryKeyColumns.get(columnName.split("\\.")[0].trim()) != null) {
                for (String tablePrimaryColumn : tablePrimaryKeyColumns.get(columnName.split("\\.")[0].trim())) {
                    fileNameSuffixList.add(keyPairRecordValues.get(tablePrimaryColumn.trim()));
                }
            }
        }
        return getValidNameForThatFile(columnName, String.join("__", fileNameSuffixList));
    }

    private String getStringFromObjectValue(ResultSet rs, int i) throws SQLException {
        Object columnData;
        final Object objectValue = rs.getObject(i);
        if (rs.wasNull() || objectValue == null)
            columnData = new String();
        else {
            stringRecordCounter++;
            columnData = objectValue;
        }
        return getXmlValidStringData(columnData.toString(), false);
    }

    public String getXmlValidStringData(String data, boolean isClobData) {

        if (data == null) {
            return NULL_VALUE;
        } else if (isClobData) {
            return stripNonValidXMLCharacters(data);
        } else {
            return stripNonValidXMLCharacters(data.replace("&", "&amp;").replace(">", "&gt;").replace("<", "&lt;")
                    .replace("'", "&apos;").replace("\"", "&quot;")).replace(NEW_LINE, NEW_LINE_TAG).replace(TAB, TAB_TAG);
        }

    }

    private String stripNonValidXMLCharacters(String data) {
        if (data == null || ("".equals(data)))
            return NULL_VALUE;
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < data.length(); i++) {
            char c = data.charAt(i);
            if (XMLChar.isValid(c))
                out.append(c);
            else
                out.append(checkReplace(data.codePointAt(i)));
        }
        return (out.toString().trim());
    }

    private String checkReplace(int key) {
        if (charReplacement != null && charReplacement.containsKey(key + ""))
            return charReplacement.get(key + "");
        if (true) // is extraction job
            return "";
        else
            return getSymbolCode(key);  //character replacement job
    }

    public TreeMap<Integer, Integer> arMap;

    private String getSymbolCode(int codePoint) {
        if (arMap == null)
            arMap = new TreeMap<>();
        arMap.put(codePoint, (arMap.get(codePoint) == null ? 0 : arMap.get(codePoint)) + 1);
        return codePoint + "";
    }

    /**
     * Reads data from an input stream into a string. Default system encoding is
     * assumed.
     *
     * @return A string with the contents of the LOB
     */
    public BinaryData readStream(final InputStream stream, final Blob blob) {
        if (stream == null) {
            return null;
        } else if (showLobs) {
            final BufferedInputStream in = new BufferedInputStream(stream);
            final BinaryData lobData = new BinaryData(readFully(in), blob);
            return lobData;
        } else {
            return new BinaryData();
        }
    }


    private String getDateValue(String value) {
        try {
            StringBuffer sb = new StringBuffer();
            if (value != null) {
                try {
                    String part1 = value.split(" ")[0];
                    String part2 = "";
                    try {
                        part2 = value.split(" ")[1];
                    } catch (Exception e) {
                        part2 = "00:00:00";
                    }
                    sb.append(part1 + " " + part2.substring(0, 8));
                } catch (Exception e) {
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    public String getCdataString(Object columnData) {
        String data = "<![CDATA[]]>";
        try {
            if (columnData == null)
                data = "<![CDATA[]]>";
            else if (columnData instanceof BinaryData) {
                columnData = ((BinaryData) columnData).toString();
                columnData = getXmlValidStringData(columnData.toString(), true);
            }
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.newDocument();
            Element rootElement = (Element) document.createElement("_");
            document.appendChild(rootElement);
            rootElement.appendChild(document.createCDATASection((String) columnData));
            String xmlString = getXMLFromDocument(document);
            if (data.length() > 4) {
                data = xmlString.substring(0, xmlString.length() - 4);
            }
            if (data.length() > 3) {
                data = data.substring(3);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data.replace(NEW_LINE, NEW_LINE_TAG).replace(TAB, TAB_TAG);
    }

    public String getXMLFromDocument(Document document) throws TransformerException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StreamResult streamResult = new StreamResult(new StringWriter());
        DOMSource source = new DOMSource(document);
        transformer.transform(source, streamResult);
        return streamResult.getWriter().toString();
    }


    public BinaryData readClob(final Clob clob, final Blob blob) {
        if (clob == null) {
            return null;
        } else if (showLobs) {
            Reader rdr = null;
            BinaryData lobData;
            try {
                try {
                    rdr = clob.getCharacterStream();
                } catch (final SQLFeatureNotSupportedException e) {
                    rdr = null;
                }
                if (rdr == null) {
                    try {
                        rdr = new InputStreamReader(clob.getAsciiStream());
                    } catch (final SQLFeatureNotSupportedException e) {
                        rdr = null;
                    }
                }

                if (rdr != null) {
                    String lobDataString = readFully(rdr);
                    if (lobDataString.isEmpty()) {
                        // Attempt yet another read
                        final long clobLength = clob.length();
                        lobDataString = clob.getSubString(1, (int) clobLength);
                    }
                    lobData = new BinaryData(lobDataString, blob);
                } else {
                    lobData = new BinaryData();
                }
            } catch (final SQLException e) {
                lobData = new BinaryData();
            }
            return lobData;
        } else {
            return new BinaryData();
        }
    }

    /**
     * Reads the stream fully, and returns a byte array of data.
     *
     * @param reader Reader to read.
     * @return Byte array
     */
    public static String readFully(final Reader reader) {
        if (reader == null) {
            return "";
        }

        try {
            final StringWriter writer = new StringWriter();
            copy(reader, writer);
            writer.close();
            return writer.toString();
        } catch (final IOException e) {
            return "";
        }

    }

    /**
     * Reads the stream fully, and writes to the writer.
     *
     * @param reader Reader to read.
     * @return Byte array
     */
    public static void copy(final Reader reader, final Writer writer) {
        if (reader == null) {
            return;
        }
        if (writer == null) {
            return;
        }

        final char[] buffer = new char[0x10000];
        try {
            // Do not close resources - that is the responsibility of the
            // caller
            final Reader bufferedReader = new BufferedReader(reader, buffer.length);
            final BufferedWriter bufferedWriter = new BufferedWriter(writer, buffer.length);

            int read;
            do {
                read = bufferedReader.read(buffer, 0, buffer.length);
                if (read > 0) {
                    bufferedWriter.write(buffer, 0, read);
                }
            } while (read >= 0);

            bufferedWriter.flush();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public BinaryData readBlob(final Blob blob) {
        if (blob == null) {
            return null;
        } else if (showLobs) {
            InputStream in = null;
            BinaryData lobData;
            try {
                try {
                    in = blob.getBinaryStream();
                } catch (final SQLFeatureNotSupportedException e) {
                    in = null;
                }

                if (in != null) {
                    lobData = new BinaryData(readFully(in), blob);
                } else {
                    lobData = new BinaryData();
                }
            } catch (final SQLException e) {
                lobData = new BinaryData();
            }
            return lobData;
        } else {
            return new BinaryData();
        }
    }

    public static String readFully(final InputStream stream) {
        if (stream == null) {
            return null;
        }
        final Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        return readFully(reader);
    }

    public long getTotalStringRecordWriteTime() {
        return totalStringRecordWriteTime;
    }

    public long getTotalBlobWriteTime() {
        return totalBlobWriteTime;
    }

    public long getTotalClobWriteTime() {
        return totalClobWriteTime;
    }

    public long getStringRecordCounter() {
        return stringRecordCounter;
    }

    public long getClobCounter() {
        return clobCounter;
    }

    public long getBlobCounter() {
        return blobCounter;
    }

    public void setTablePrimaryKeyColumns(Map<String, List<String>> tablePrimaryKeyColumns) {
        this.tablePrimaryKeyColumns = tablePrimaryKeyColumns;
    }
}
