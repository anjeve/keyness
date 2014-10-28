package de.hpi.fgis.loducc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.hp.hpl.jena.ontology.AllValuesFromRestriction;
import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntProperty;
import com.hp.hpl.jena.ontology.OntResource;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

import bb.util.Benchmark;

public class Statistics {
	private Model dataset = ModelFactory.createDefaultModel();
	private static final Logger logger = Logger.getLogger(LODUCC.class);
	private boolean resume = false;
	private boolean all = false;
	private HashMap<String, String> classPropertiesDone;
	
	public Statistics(String[] files) throws IllegalArgumentException, IllegalStateException, Exception {
		PropertyConfigurator.configure("log4j.properties");
		logger.setLevel(Level.WARN);
		//BasicConfigurator.configure();
		/*
		long t1 = System.currentTimeMillis();
		task.run();    // task is a Runnable which encapsulates the unit of work
		long t2 = System.currentTimeMillis();
		System.out.println("My task took " + (t2 - t1) + " milliseconds to execute.");
		*/
		
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < files.length; i++) {
			loadDump(files[i]);
		}
		long t2 = System.currentTimeMillis();
		//logger.warn("Loading the dataset took " + (t2 - t1) + " milliseconds to execute.");
		
		getUniqueness();
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

	private Boolean getUniqueness() {
		OntModel ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, dataset);

		long t1 = System.currentTimeMillis();

		for (Iterator<OntClass> i = ontology.listClasses(); i.hasNext();) {
			OntClass ontologyClass = i.next();
            if (!ontologyClass.getURI().toString().startsWith("http://dbpedia.org/ontology") && !ontologyClass.getURI().toString().equals("http://www.w3.org/2002/07/owl#Thing")) continue;

            int entityCount = 0;
            for (ExtendedIterator<Individual> j = ontology.listIndividuals(ontologyClass); j.hasNext(); ++entityCount ) j.next();
            
            System.out.println(ontologyClass.getURI()+","+entityCount);
		}
		
		return true;
	}
	
	private void loadDump(String file) throws Exception {
		Model dataset1 = FileManager.get().loadModel(file);
		dataset.add(dataset1);
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
			new Statistics(args);
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
