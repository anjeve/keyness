package de.hpi.fgis.loducc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvReader {
	
	public CsvReader(String[] files) throws IllegalArgumentException, IllegalStateException, Exception {
		for (int i = 0; i < files.length; i++) {
			readCsv(files[i]);
		}
	}

	public static List<String[]> readCsv(String csvFileToRead) throws Exception {
		List<String[]> result = new ArrayList<String[]>();
		BufferedReader br = null;
		String line = "";
		String splitBy = ",";
		int csvLength = 0;
		int lineNr = 1;
		try {
			br = new BufferedReader(new FileReader(csvFileToRead));
			while ((line = br.readLine()) != null) {
				String[] csvLine = line.split(splitBy);
				result.add(csvLine);
				if (csvLength == 0) {
					csvLength = csvLine.length;
				} else {
					if (csvLength != csvLine.length) {
						if ((line.length() - line.replace(splitBy, "").length()) != (csvLength -1)) {
							throw new IllegalStateException("Line "+lineNr+" has " + csvLine.length + " (instead of "+csvLength+") columns:\n"+line);
						}
					}
				}
			}
			lineNr += 1;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	public static void main(String[] args) {
		try {
			new CsvReader(args);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
