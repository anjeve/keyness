package de.hpi.fgis.loducc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntProperty;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class Ontology2CsvConverter {
	final OntModel ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);

	private boolean resume = false;
	private boolean threads = true;
	private HashMap<String, String> classPropertiesDone;
	private List<String> classRestrictions = new ArrayList<String>();
	boolean all = false;
	
	public Ontology2CsvConverter(String[] files) throws IllegalArgumentException, IllegalStateException, Exception {
		//BasicConfigurator.configure();
		/*
		long t1 = System.currentTimeMillis();
		task.run();    // task is a Runnable which encapsulates the unit of work
		long t2 = System.currentTimeMillis();
		System.out.println("My task took " + (t2 - t1) + " milliseconds to execute.");
		*/
		
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < files.length; i++) {
			if (files[i].equals("resume")) {
				this.resume = true;
				this.classPropertiesDone = readCsv("dbpedia_uniqueness.csv");
			} else if (files[i].startsWith("-class=")) {
				this.classRestrictions.add(files[i].replaceAll("-class=", ""));
			} else if (files[i].equals("nothreads")) {
				this.threads = false;
			} else {
				loadDump(files[i]);
			}
		}
		long t2 = System.currentTimeMillis();
		System.out.println("Loading the dataset took " + (t2 - t1) + " milliseconds to execute.");
		
		convert();
/*
	    Callable<Boolean> task = 
		        new Callable<Boolean>() { public Boolean call() { return getUniqueness(); } };
		    System.out.println("getUniqueness(): " + new Benchmark(task));
*/
		//getProperties();
		//getCLasses();
		/*
		
	    Callable<Integer> task = 
	        new Callable<Integer>() { public Integer call() { return fibonacci(35); } };
	    System.out.println("fibonacci(35): " + new Benchmark(task));
*/
	}

	private Boolean convert() throws FileNotFoundException {
		long t1 = System.currentTimeMillis();

		final List<String> properties = new ArrayList<String>();
		ExtendedIterator<OntProperty> propertyIterator = ontology.listAllOntProperties();
		while (propertyIterator.hasNext()) {
			OntProperty property = (OntProperty) propertyIterator.next();
			properties.add(property.getURI());
		}
		System.out.println(properties.size() + " properties loaded.");
		
		if (!this.classRestrictions.isEmpty()) {
			for (Iterator<String> iterator = this.classRestrictions.iterator(); iterator.hasNext();) {
				String className = (String) iterator.next();
				final OntClass ontologyClass;
				if (className.equals("owl:Thing")) {
					ontologyClass = ontology.getOntClass("http://www.w3.org/2002/07/owl#Thing");
					all = true;
				    for (Iterator<OntClass> i = ontology.listClasses(); i.hasNext();) {
						final OntClass ontologyClassAll = i.next();
			            getUniquenessForClass(properties, ontologyClassAll);
				    }			

				} else {
					ontologyClass = ontology.getOntClass("http://dbpedia.org/ontology/"+className);
					getUniquenessForClass(properties, ontologyClass);
				}
			}
		} else {
		    for (Iterator<OntClass> i = ontology.listClasses(); i.hasNext();) {
				final OntClass ontologyClass = i.next();
	            getUniquenessForClass(properties, ontologyClass);
		    }			
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println("Converting the ontology to CSV took " + (t2 - t1) + " milliseconds to execute.");

		return true;
	}

	private void getUniquenessForClass(final List<String> properties, final OntClass ontologyClass) {
		if (!ontologyClass.getURI().startsWith("http://dbpedia.org/ontology") && !ontologyClass.getURI().toString().equals("http://www.w3.org/2002/07/owl#Thing")) {
			return;
		} 
		String className = "";
		if (all) {
			className = "http://www.w3.org/2002/07/owl#Thing";
		} else {
			className = ontologyClass.getURI();
			while (Thread.activeCount() > 10) {
				Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
				System.out.print("currently active threads:");
				for (Iterator<Thread> iterator = threadSet.iterator(); iterator.hasNext();) {
					Thread thread = (Thread) iterator.next();
					if (thread.getName().startsWith("http://")) {
						System.out.print(" " + thread.getName());
					}
				}
				System.out.println();
				try {
					Thread.sleep(600);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		if (this.threads) {
			new Thread(className){
				public void run() {
					System.out.println("Starting thread: " + getName());
	
					new ClassConverter(getName(), ontology, properties, ontologyClass);
					//x(properties, ontologyClass);
					System.out.println("Finished thread: " + getName());


				}
			}.start();
			Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
			System.out.print("Currently active threads:");
			for (Iterator iterator = threadSet.iterator(); iterator.hasNext();) {
				Thread thread = (Thread) iterator.next();
				if (thread.getName().startsWith("http://")) {
					System.out.print(" " + thread.getName());
				}
			}
			System.out.println();
		} else {
			new ClassConverter(className, ontology, properties, ontologyClass);
		}
	}
	
	private void loadDump(String file) throws Exception {
		OntModel dataset = ModelFactory.createOntologyModel();
		FileManager.get().readModel(dataset, file);
		ontology.add(dataset);
		System.out.println("Loaded "+file);
	}
	
	public HashMap<String, String> readCsv(String csvFileToRead) {
		HashMap<String, String> csv = new HashMap<String, String>();
		BufferedReader br = null;
		String line = "";
		String splitBy = ",";

		try {
			br = new BufferedReader(new FileReader(csvFileToRead));
			while ((line = br.readLine()) != null) {
				String[] csvLine = line.split(splitBy);
				if (csvLine.length == 3) {
					csv.put(csvLine[0], csvLine[1]);
				}
			}
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
		return csv;
	}

	public static void main(String[] args) {
		try {
			new Ontology2CsvConverter(args);
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
