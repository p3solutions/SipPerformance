package com.p3.archon.sip_process.core;

import com.p3.archon.sip_process.bean.BinaryData;
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

import static com.p3.archon.sip_process.constants.FileNameConstant.ATTACHMENT_FOLDER;

/**
 * Created by Suriyanarayanan K
 * on 14/05/20 2:23 PM.
 */
public class RowObjectCreator {

    private ResultSetMetaData resultSetMetaData;
    private String NULL_VALUE = "NULL VALUE";
    private boolean showLobs = true;

    private long totalStringRecordWriteTime = 0;
    private long totalBlobWriteTime = 0;
    private long totalClobWriteTime = 0;

    private long stringRecordCounter = 0;
    private long clobCounter = 0;
    private long blobCounter = 0;
    private String attachmenFolderName;
    private TreeMap<String, String> charReplacement;
    private boolean isShowDateTime;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
    private DecimalFormat formatter;
    private String DECIMAL_PATTERN = "#.##########################################################################################################################################################################################################################";

    public RowObjectCreator(ResultSetMetaData resultSetMetaData, String attachmentLocation, TreeMap<String, String> charReplacement, boolean isShowDateTime) {
        this.resultSetMetaData = resultSetMetaData;
        this.attachmenFolderName = attachmentLocation + File.separator + ATTACHMENT_FOLDER;
        new File(attachmenFolderName).mkdirs();
        this.charReplacement = charReplacement;
        this.isShowDateTime = isShowDateTime;
    }

    public List<String> getRowObjectList(ResultSet rs) throws SQLException {
        List<String> rowObject = new ArrayList<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
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
                    rowObject.add(getCdataString(columnData));
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
                    rowObject.add((getCdataString(columnData)));
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
                    rowObject.add(getBlobInfo(columnData));
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
                    rowObject.add(getBlobInfo(columnData));
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
                        rowObject.add(getDateValue(columnData.toString()));
                    } else
                        rowObject.add(getXmlValidStringData(columnData.toString()));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                case Types.TIMESTAMP:
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    startTimeRec = System.currentTimeMillis();
                    final Timestamp timestampValue = rs.getTimestamp(i);
                    if (rs.wasNull() || timestampValue == null) {
                        columnData = new String();
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
                    }
                    rowObject.add(getDateValue(columnData.toString()));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
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
                    rowObject.add(getXmlValidStringData(columnData.toString()));
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
                    rowObject.add(getXmlValidStringData(columnData.toString()));
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
                    rowObject.add(getXmlValidStringData(columnData.toString()));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
                    break;
                default:
                    startTimeRec = System.currentTimeMillis();
                    rowObject.add(getStringFromObjectValue(rs, i));
                    totalStringRecordWriteTime += (System.currentTimeMillis() - startTimeRec);
            }
        }
        return rowObject;
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
        return getXmlValidStringData(columnData.toString());
    }

    public String getXmlValidStringData(String data) {
        if (data == null)
            return NULL_VALUE;
        return stripNonValidXMLCharacters(data.replace("&", "&amp;").replace(">", "&gt;").replace("<", "&lt;")
                .replace("'", "&apos;").replace("\"", "&quot;"));
    }

    private String stripNonValidXMLCharacters(String in) {
        if (in == null || ("".equals(in)))
            return null;
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            if (XMLChar.isValid(c))
                out.append(c);
            else
                out.append(checkReplace(in.codePointAt(i)));
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

    private String getBlobInfo(Object columnData) {
        if (columnData == null)
            return NULL_VALUE;
        else {
            try {
                String validFileName = UUID.randomUUID().toString().substring(0, 14) + new Date().getTime();
                String fileName = attachmenFolderName + File.separator + validFileName;
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
                return validFileName; // + type;
            } catch (Exception e) {
                e.printStackTrace();
                return NULL_VALUE;
            }
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
            else if (columnData instanceof BinaryData)
                columnData = ((BinaryData) columnData).toString();

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
        return data;
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
}
