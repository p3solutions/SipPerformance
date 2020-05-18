/*
 * CoreSettings.java
 * Created on Oct 17, 2014
 * 
 * This class wraps a 'Properties' object.  It provides the same functions as the Properties
 * class but also adds helper functions to return values as int, String[] and boolean.
 * 
 * This class is the parent class to CoreSDOSettings and CoreJSFSettings.
 */
package com.p3.archon.sip_process.core;

import java.io.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

public class PropertyFileReader implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * This holds all the key/value pairs read in from a .properties file.
	 */
	private Properties properties;

	/**
	 * This constructor is here to satisfy class requirements, but is not intended
	 * to be used. The CoreSettings(String filename) version should be used.
	 *
	 */
	protected PropertyFileReader() {

	}

	/**
	 * Constructor loads settings from property file into 'properties' class
	 * variable. This version accepts a String that contains a path and filename to
	 * the property file.
	 *
	 * @param filename
	 *            Path and file name to the .properties file to load
	 */
	public PropertyFileReader(URL urlFileName) {
		try {
			InputStream in = urlFileName.openStream();
			properties = new Properties();
			properties.load(in);
		} catch (IOException ioe) {
			System.out.println("Unable to open " + urlFileName.getFile() + " due to " + ioe.getMessage());
		}

	}

	/**
	 * Constructor loads settings from property file into 'properties' class
	 * variable. This version accepts a String that contains a path and filename to
	 * the property file.
	 * 
	 * @param filename
	 *            Path and file name to the .properties file to load
	 */
	public PropertyFileReader(String filename, String option) {
		InputStream inputStream = null;
		try {
			properties = new Properties();
			inputStream = getClass().getClassLoader().getResourceAsStream(filename);
			properties.load(inputStream);
		} catch (IOException ioe) {
			System.out.println("Unable to open " + filename + " due to " + ioe.getMessage());
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (Exception e) {
			}
		}
	}

	public PropertyFileReader(String filename) {
		FileInputStream fileInputStream = null;
		try {
			properties = new Properties();
			File file = new File(filename);
			fileInputStream = new FileInputStream(file);
			properties.load(fileInputStream);
		} catch (IOException ioe) {
			System.out.println("Unable to open " + filename + " due to " + ioe.getMessage());
		} finally {
			try {
				if (fileInputStream != null)
					fileInputStream.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * Looks up keyName and returns the value as a boolean. Format of property file
	 * is: <br>
	 * <br>
	 * keyName=value <br>
	 * true is 1,Y,y,Yes,yes,True,true,T,t <br>
	 * false is 0,N,n,No,no,False,false,F,f <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * 
	 * @return boolean value
	 */
	public boolean getBooleanValue(String keyName) {
		String strValue;
		boolean result = false;
		strValue = getValue(keyName, "0");
		result = isResult(strValue, false);
		return result;
	}

	/**
	 * Looks up keyName and returns the value as a boolean. If not found it will use
	 * 'defaultValue' passed in instead. Format of property file is: <br>
	 * <br>
	 * keyName=value <br>
	 * true is 1,Y,y,Yes,yes,True,true,T,t <br>
	 * false is 0,N,n,No,no,False,false,F,f <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * 
	 * @return boolean value
	 */
	public boolean getBooleanValue(String keyName, boolean defaultValue) {
		String strValue;
		boolean result = defaultValue;
		strValue = getValue(keyName, "x");
		result = isResult(strValue, result);
		if (strValue.startsWith("0"))
			result = false;
		if (strValue.startsWith("N"))
			result = false;
		if (strValue.startsWith("n"))
			result = false;
		if (strValue.startsWith("F"))
			result = false;
		if (strValue.startsWith("f"))
			result = false;
		return result;
	}

	private boolean isResult(String strValue, boolean result) {
		if (strValue.startsWith("1"))
			result = true;
		if (strValue.startsWith("Y"))
			result = true;
		if (strValue.startsWith("y"))
			result = true;
		if (strValue.startsWith("T"))
			result = true;
		if (strValue.startsWith("t"))
			result = true;
		return result;
	}

	/**
	 * Looks up keyName and returns the value as an integer. Format of property file
	 * is: <br>
	 * <br>
	 * keyName=value <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * 
	 * @return int value
	 */
	public int getIntegerValue(String keyName) {
		int i;
		try {
			i = Integer.parseInt(getValue(keyName, "0"));
		} catch (NumberFormatException e) {
			i = 0;
		}
		return i;
	}

	/**
	 * Looks up keyName and returns the value as an integer. If not found, will
	 * return 'defaultValue' passed in. Format of property file is: <br>
	 * <br>
	 * keyName=value <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * @param defaultValue
	 *            If this key is not found in the property file, return this value
	 * 
	 * @return int value
	 */
	public int getIntegerValue(String keyName, int defaultValue) {
		int i;
		try {
			i = Integer.parseInt(getValue(keyName, Integer.toString(defaultValue)));
		} catch (NumberFormatException e) {
			i = defaultValue;
		}
		return i;
	}

	/**
	 * Looks up keyName and returns the value as a string array. Format of property
	 * file is: <br>
	 * <br>
	 * keyName=value <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * 
	 * @return String[] value
	 */
	public String[] getStringArray(String keyName) {
		StringTokenizer array = new StringTokenizer(getStringValue(keyName), ",", false);
		String[] nameArray = new String[array.countTokens()];
		for (int i = 0; i < nameArray.length; i++) {
			nameArray[i] = array.nextToken();
		}
		return nameArray;
	}

	/**
	 * Looks up keyName and returns the value as a String. Format of property file
	 * is: <br>
	 * <br>
	 * keyName=value <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * 
	 * @return String value
	 */
	public String getStringValue(String keyName) {
		return getValue(keyName, "");
	}

	/**
	 * Looks up keyName and returns the value as a string. If not found,
	 * 'defaultValue' will be returned instead. Format of property file is: <br>
	 * <br>
	 * keyName=value <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * @param defaultValue
	 *            If this key is not found in the property, return this value
	 * 
	 * @return String value
	 */
	public String getStringValue(String keyName, String defaultValue) {
		return getValue(keyName, defaultValue);
	}

	/**
	 * Returns an enumeration of the keys in properties.
	 */
	@SuppressWarnings("rawtypes")
	public Enumeration getKeys() {
		return properties.keys();
	}

	/**
	 * Helper function to look up keys in .properties files. Format of property file
	 * is: <br>
	 * <br>
	 * keyName=value <br>
	 * 
	 * @param keyName
	 *            Name of key to search for
	 * @param defaultValue
	 *            If this key is not found in the file, return this value
	 * 
	 * @return String value
	 */
	private String getValue(String keyName, String defaultValue) {
		String result;
		result = properties.getProperty(keyName, defaultValue);
		return result;
	}

}
