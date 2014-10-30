package de.hpi.fgis.loducc.statistics;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import de.hpi.fgis.loducc.CsvReader;

public class Keyness {

	public Keyness(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			List<String[]> csv = CsvReader.readCsv(args[i]);
			getKeyness(csv);
		}
	}
	
	private void getKeyness(List<String[]> csv) throws FileNotFoundException {
		PrintWriter out = new PrintWriter("keyness.csv");
		for (Iterator<String[]> iterator = csv.iterator(); iterator.hasNext();) {
			String[] strings = (String[]) iterator.next();
			double[] data = new double[2];
			for (int i = 0; i < strings.length; i++) {
				if (i > 2) {
					try {
						data[i-3] = Double.parseDouble(strings[i]);
					} catch(ArrayIndexOutOfBoundsException e) {
						System.out.println(strings[0]+ strings[2]);
						e.printStackTrace();
						System.exit(0);
					} finally {
						
					}
				}
				out.print(strings[i] + ",");
			}
			out.println(harmonicMean(data));
		}
		out.close();
	}

	public static double getKeyness(double uniqueness, double density) {
		double[] data = new double[2];
		data[0] = uniqueness;
		data[1] = density;
		return harmonicMean(data);
	}

	public static double harmonicMean(double[] data) {
        double sum = 0.0;
 
        for (int i = 0; i < data.length; i++) {
            sum += 1.0 / data[i];
        }
 
        return data.length / sum;
    }

	public static void main(String[] args) throws Exception {
		new Keyness(args);
	}
}