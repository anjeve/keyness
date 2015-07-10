package de.hpi.fgis.loducc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import de.hpi.fgis.loducc.statistics.Statistics;

public class LODUCC {
	private static final String OWL_THING = "http://www.w3.org/2002/07/owl#Thing";
	private static final String DBPEDIA_ONTOLOGY_NS = "http://dbpedia.org/ontology/";
	private static final String DBPEDIA_RESOURCE_NS = "http://dbpedia.org/resource/";
	private Model dataset = ModelFactory.createDefaultModel();
	private static final Logger logger = Logger.getLogger(LODUCC.class);
	public static final String JENA = "jena";
	public static final String TDB = "tdb";
	private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private boolean all = false;
	private boolean resume = false;
	private String ns = null;
	private String onlyProperty = null;
	private String ontns = null;
	private String outputLocation;
	private PrintWriter out;
	private boolean jena;
	private List<String> classes = new ArrayList<String>();
	private List<String> alreadyProfiledClasses = new ArrayList<String>();
	private boolean tdb = false;
	private OntModel ontology;
	private String only;
	private boolean datasetLoadedAsOnt;
	private String resumeLocation;
	
	/**
	 * @param files
	 * @param mode
	 * @throws IllegalArgumentException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public LODUCC(String[] files, String mode) throws IllegalArgumentException, IllegalStateException, Exception {
		PropertyConfigurator.configure("log4j.properties");
		logger.setLevel(Level.INFO);

		if (mode.equals(JENA)) {
			logger.info("Jena version");
			this.jena  = true;
		} else if (mode.equals(TDB)) {
			logger.info("TDB version");
			this.tdb  = true;
		}

		long t1 = System.currentTimeMillis();
		
		int loadedDatasets = 0;
		
		for (int i = 0; i < files.length; i++) {
			if (files[i].equals("debug")) {
				logger.setLevel(Level.DEBUG);
			} else if(files[i].startsWith("-resume=")) {
				this.resume = true;
				this.resumeLocation = files[i].replaceFirst("-resume=", "");
			} else if (files[i].equals("owl:Thing")) {
				this.all = true;
			} else if (files[i].startsWith("-only=")) {
				this.only = files[i].replaceFirst("-only=", "");
			} else if(files[i].startsWith("-ontology=")) {
				String ontologyLocation = files[i].replaceFirst("-ontology=", "");
				Model ontologyModel = FileManager.get().loadModel(ontologyLocation);
				this.ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, ontologyModel);
			} else if(files[i].startsWith("-ontns=")) {
				this.ontns = files[i].replaceFirst("-ontns=", "");
			} else if(files[i].startsWith("-ns=")) {
				this.ns = files[i].replaceFirst("-ns=", "");
			} else if(files[i].startsWith("-property=")) {
				this.onlyProperty = files[i].replaceFirst("-property=", "");
			} else if (files[i].startsWith("-out=")) {
				this.outputLocation = files[i].replaceFirst("-out=", "");
			} else {
				if (this.tdb) {
					loadTdb(files[i], loadedDatasets);
				} else if (this.jena) {
					Model model = loadDump(files[i]);
					this.dataset.add(model);
				}
	            logger.info("Dataset loaded: " + files[i]);
				loadedDatasets++;
			}
		}
		
		// TODO onlyProperty
		
		if (loadedDatasets == 0) {
			throw new IllegalArgumentException("Provide at least one dataset to load.");
		}

		if (this.ns == null) {
			this.ns = DBPEDIA_RESOURCE_NS;
		}
		if (this.ontns == null) {
			this.ontns = DBPEDIA_ONTOLOGY_NS;
		}

		if (this.ontology == null) {
			this.ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, this.dataset);
			this.datasetLoadedAsOnt = true;
		}
		
		if (this.outputLocation == null) {
			// TODO create useful output file name
			this.outputLocation = "output.csv";
		}

		if ((this.resume) && (this.resumeLocation != null)) {
			readCsv(this.resumeLocation);
			logger.info("Resuming " + this.resumeLocation);
		}
		
		if (this.out == null) {
			// TODO check if file available
			if (this.resume && (this.outputLocation == null)) {
				this.outputLocation = this.resumeLocation.replace(".csv", "_resumed.csv");
			}
			this.out = new PrintWriter(this.outputLocation);
			logger.info("Output: "+this.outputLocation);
		}
		
		long t2 = System.currentTimeMillis();
		logger.info("Loading the dataset took " + (t2 - t1) + " ms.");
		
		long t3 = System.currentTimeMillis();
		if (this.only != null) {
			if (this.ontology == null) throw new IllegalArgumentException("Missing ontology.");
			logger.info("Analyzing only entities of type " + this.ontns + this.only);
			profileKeyness(this.ontns+this.only);
		} else {
			profileKeyness();
		}
		long t4 = System.currentTimeMillis();
		logger.info("Getting the uniqueness, density, and keyness took " + (t4 - t3) + " ms.");

		this.dataset.close();
		if (this.out != null) {
			this.out.close();
		}
	}

	/**
	 * @throws FileNotFoundException
	 */
	private void profileKeyness() throws FileNotFoundException {
		if (this.all) {
			profileKeynessForAllEntities();
		} else {
			for (Iterator<OntClass> i = this.ontology.listClasses(); i.hasNext();) {
				OntClass ontologyClass = i.next();
				String classUri = ontologyClass.getURI();
				if (!classUri.startsWith(this.ontns)) continue;
				if (this.resume && this.alreadyProfiledClasses.contains(classUri)) {
					logger.debug("Skipping class " + ontologyClass.getURI());
					continue;
				}
				profileKeyness(ontologyClass);
			}
		}
	}
	
	/**
	 * 
	 * @param classUri
	 * @throws FileNotFoundException
	 */
	private void profileKeyness(OntClass ontologyClass) throws FileNotFoundException {
		if (all) {
			profileKeynessForAllEntities();
		} else {
			logger.debug("Profiling class " + ontologyClass.getURI());
			HashSet<String> subjects = getEntityUris(ontologyClass);
            outputKeynessStatistics(ontologyClass, subjects);
        } 
	}

	/**
	 * 
	 * @param classUri
	 * @throws FileNotFoundException
	 */
	private void profileKeyness(String classUri) throws FileNotFoundException {
		if (all) {
			profileKeynessForAllEntities();
		} else {
			Resource ontologyClass = dataset.getResource(classUri);
			OntClass ontologyClassObject = this.ontology.getOntClass(ontologyClass.getURI());
			profileKeyness(ontologyClassObject);
        } 
	}
	


	/**
	 * 
	 */
	private void profileKeynessForAllEntities() {
		HashSet<String> subjects = new HashSet<String>();
		String sparql_s = "SELECT DISTINCT ?s " + "WHERE { ";
		if (this.ns.equals(DBPEDIA_RESOURCE_NS)) {
			sparql_s += "?s a <" + OWL_THING + ">. } ";
		} else {
			sparql_s += "?s a ?t. } ";
		}
		Query qry_s = QueryFactory.create(sparql_s);
		QueryExecution qe_s = QueryExecutionFactory.create(qry_s, dataset);
		ResultSet rs_s = qe_s.execSelect();
		while (rs_s.hasNext()) {
			QuerySolution sol = rs_s.nextSolution();
			RDFNode subject = sol.get("s");
			String subjectUri = subject.toString();
			if (!subjectUri.startsWith(this.ns)) continue;
			// TODO not needed? if (subjectUri.startsWith(this.ontns)) continue;
			subjects.add(subjectUri);
		}
		qe_s.close();
		logger.info("Found " + subjects.size() + " entities of type owl:Thing");
		outputKeynessStatistics(this.ontology.getOntClass(OWL_THING), subjects);
	}

	/**
	 * @param ontologyClass
	 * @param subjects
	 */
	private void outputKeynessStatistics(OntClass ontologyClass, HashSet<String> subjects) {
		HashMap<String, HashMap<Key, Integer>> propertiesPropertyValueMap = new HashMap<String, HashMap<Key, Integer>>();
		for (Iterator<String> iterator = subjects.iterator(); iterator.hasNext();) {
			String subject = iterator.next();
			
			String sparql = "SELECT ?p ?o " + "WHERE { "
				+ "<"+subject + "> ?p ?o. } ORDER BY ?p ";
			
			Query qry = QueryFactory.create(sparql);
			QueryExecution qe = QueryExecutionFactory.create(qry, dataset);
			ResultSet rs = qe.execSelect();

			//HashMap<Key, Integer> propertyValues = new HashMap<Key, Integer>();
			
			boolean firstProperty = true;
			String currentProperty = "";
			List<String> currentPropertyValues = new ArrayList<String>();
			while (rs.hasNext()) {
				QuerySolution sol = rs.nextSolution();
				RDFNode property = sol.get("p");
				RDFNode object = sol.get("o");
				String propertyUri = property.toString();
				if (!currentProperty.equals(propertyUri)) {
					if (firstProperty) {
						firstProperty = false;
					} else {
						Key keys = new Key(currentPropertyValues);
						if (propertiesPropertyValueMap.containsKey(currentProperty)) {
							HashMap<Key, Integer> existingPropertyValues = propertiesPropertyValueMap.get(currentProperty);
							existingPropertyValues = addPropertyValueCount(existingPropertyValues, keys);
							propertiesPropertyValueMap.put(currentProperty, existingPropertyValues);
						} else {
							HashMap<Key, Integer> propertyValues = new HashMap<Key, Integer>();
							propertyValues = addPropertyValueCount(propertyValues, keys);
							propertiesPropertyValueMap.put(currentProperty, propertyValues);
						}
					}
					currentProperty = propertyUri;
					currentPropertyValues = new ArrayList<String>();
				} 
				currentPropertyValues.add(object.toString());
			}
			// add last property
			if (currentPropertyValues.size() > 0) {
				Key keys = new Key(currentPropertyValues);
				if (propertiesPropertyValueMap.containsKey(currentProperty)) {
					HashMap<Key, Integer> existingPropertyValues = propertiesPropertyValueMap.get(currentProperty);
					existingPropertyValues = addPropertyValueCount(existingPropertyValues, keys);
					propertiesPropertyValueMap.put(currentProperty, existingPropertyValues);
				} else {
					HashMap<Key, Integer> propertyValues = new HashMap<Key, Integer>();
					propertyValues = addPropertyValueCount(propertyValues, keys);
					propertiesPropertyValueMap.put(currentProperty, propertyValues);
				}
			}			
			qe.close();
		}
		
		for (Iterator iterator = propertiesPropertyValueMap.keySet().iterator(); iterator.hasNext();) {
			String propertyUri = (String) iterator.next();
			HashMap<Key, Integer> propertyValues = propertiesPropertyValueMap.get(propertyUri);

			Statistics stats = new Statistics();
			if (this.all) {
				stats.getUniquenessDensityKeyness(OWL_THING,
						propertyUri, propertyValues, subjects.size(), this.out);
			} else {
				stats.getUniquenessDensityKeyness(ontologyClass.getURI(), propertyUri,
						propertyValues, subjects.size(), this.out);
			}
		}
	}

	private HashMap<Key, Integer> addPropertyValueCount(HashMap<Key, Integer> propertyValues, Key key) {
		if (!propertyValues.containsKey(key)) {
			propertyValues.put(key, 1);
		} else {
			propertyValues.put(key, propertyValues.get(key) + 1);
		}
		return propertyValues;
	}

	/**
	 * @param ontologyClass
	 * @return
	 */
	private HashSet<String> getEntityUris(OntClass ontologyClass) {
		String ontologyClassUri = ontologyClass.getURI();
		HashSet<String> entityURLs = new HashSet<String>();
		String sparql = "SELECT ?s ?t " + "WHERE { ";
		// if DBpedia, get subclasses already from SPARQL
		if (this.ns.equals(DBPEDIA_RESOURCE_NS)) {
			sparql += " ?s a <" + ontologyClassUri + ">; ";
			sparql += " a ?t. } ";
		} else {
			sparql += " ?s a <" + ontologyClassUri + ">. } ";
		}
		// get subclasses
		List<OntClass> subclasses = new ArrayList<OntClass>();
		ExtendedIterator<OntClass> subclassIterator;
		if (this.only != null) {
			subclassIterator = ontologyClass.listSubClasses(true);
		} else {
			subclassIterator = ontologyClass.listSubClasses();
		}
		while (subclassIterator.hasNext()) {
		  OntClass subclass = subclassIterator.next();
		  if (!subclass.toString().startsWith(this.ontns)) continue;
		  subclasses.add(subclass);
		}
		if (subclasses.size() > 0) {
			logger.debug("  Subclasses: " + StringUtils.join(subclasses,","));
		}
		
		List<String> toDelete = new ArrayList<String>();

		// if the whole dataset is loaded as ontology load instances from it
		if (this.datasetLoadedAsOnt) {
			/*
			ExtendedIterator it = ontologyClass.listInstances();
			while (it.hasNext()) {
				Individual entity = (Individual) it.next();
				if (!entity.toString().startsWith(this.ns)) continue;
				if (!entityURLs.contains(entity.toString()) && !toDelete.contains(entity.toString())) {
					entityURLs.add(entity.toString());
				}
				if (this.only != null) {
					// TODO handle this
				}
			}
			*/
			entityURLs = listEntities(ontologyClassUri);
		// otherwise use SPARQL
		} else {
			Query qry = QueryFactory.create(sparql);
			QueryExecution qe = QueryExecutionFactory.create(qry, this.dataset);
			ResultSet rs = qe.execSelect();
	
			while (rs.hasNext()) {
				QuerySolution sol = rs.nextSolution();
				RDFNode subject = sol.get("s");
				RDFNode type = sol.get("t");
				if (!subject.toString().startsWith(this.ns)) continue;
				if (!entityURLs.contains(subject.toString()) && !toDelete.contains(subject.toString())) {
					entityURLs.add(subject.toString());
				}
				if (this.only != null) {
					if (subclasses.contains(type.toString())) {
						if (!toDelete.contains(subject.toString())) {
							toDelete.add(subject.toString());
						}
					}
				}
			}
			qe.close();
		}
		
		// if -only parameter given, exclude entities of subclasses
		if (this.only != null) {
			if (!this.ns.equals(DBPEDIA_RESOURCE_NS)) {
				for (Iterator<OntClass> iterator = subclasses.iterator(); iterator.hasNext();) {
					OntClass subclassClass = (OntClass) iterator.next();
					String subclass = subclassClass.getURI();
					logger.debug("Excluding entities of type " + subclass);
					
					if (this.datasetLoadedAsOnt) {
						/*
						ExtendedIterator it = subclassClass.listInstances();
						while (it.hasNext()) {
							Individual entity = (Individual) it.next();
							if (!entity.getURI().startsWith(this.ns)) continue;
							if (!toDelete.contains(entity.getURI())) {
								toDelete.add(entity.getURI());
							}
						}
						*/
						HashSet<String> subclassIndividuals = listEntities(subclass);
						for (Iterator iterator2 = subclassIndividuals.iterator(); iterator2.hasNext();) {
							String individualUri = (String) iterator2.next();
							if (!toDelete.contains(individualUri)) {
								toDelete.add(individualUri);
							}
						}
					} else {
						String sparql_subclass = "SELECT ?s ?t " + "WHERE { "
								+ " ?s a <" + subclass + ">. }";
						Query qry_subclass = QueryFactory.create(sparql_subclass);
						QueryExecution qe_subclass = QueryExecutionFactory.create(qry_subclass, this.dataset);
						ResultSet rs_subclass = qe_subclass.execSelect();
						while (rs_subclass.hasNext()) {
							QuerySolution sol_subclass = rs_subclass.nextSolution();
							RDFNode subject_subclass = sol_subclass.get("s");
							if (!subject_subclass.toString().startsWith(this.ns)) continue;
							if (!toDelete.contains(subject_subclass.toString())) {
								toDelete.add(subject_subclass.toString());
							}
		
						}
						qe_subclass.close();
					}
				}
			}
			
		} else {	// add entities of subclasses (if not DBpedia and all classes given already)
			if (entityURLs.size() > 0) {
				logger.info("Found " + entityURLs.size() + " entities of type " + ontologyClassUri);
			}
			if (!this.ns.equals(DBPEDIA_RESOURCE_NS)) {
				for (Iterator<OntClass> iterator = subclasses.iterator(); iterator.hasNext();) {
					OntClass subclassClass = (OntClass) iterator.next();
					String subclass = subclassClass.getURI();
					
					int subclassEntityCount = 0;

					// if the whole dataset is loaded as ontology load instances from it
					if (this.datasetLoadedAsOnt) {
						ExtendedIterator it = subclassClass.listInstances();
						while (it.hasNext()) {
							Individual entity = (Individual) it.next();
							if (!entity.getURI().startsWith(this.ns)) continue;
							if (!entityURLs.contains(entity.getURI()) && !toDelete.contains(entity.getURI())) {
								entityURLs.add(entity.getURI());
								subclassEntityCount++;
							}
						}
					//otherwise use SPARQL
					} else {
						String sparql_subclass = "SELECT ?s " + "WHERE { "
								+ " ?s a <" + subclass + ">. }";
						Query qry_subclass = QueryFactory.create(sparql_subclass);
						QueryExecution qe_subclass = QueryExecutionFactory.create(qry_subclass, this.dataset);
						ResultSet rs_subclass = qe_subclass.execSelect();
						while (rs_subclass.hasNext()) {
							QuerySolution sol_subclass = rs_subclass.nextSolution();
							RDFNode subject_subclass = sol_subclass.get("s");
							if (!subject_subclass.toString().startsWith(this.ns)) continue;
							if (!entityURLs.contains(subject_subclass.toString()) && !toDelete.contains(subject_subclass.toString())) {
								entityURLs.add(subject_subclass.toString());
								subclassEntityCount++;
							}
						}
						qe_subclass.close();
					}
					if (subclassEntityCount > 0) {
						logger.debug("  Added " + subclassEntityCount + " entities of type " + subclass + " to " + ontologyClassUri);
					}
				}
			}
		}

		for (Iterator<String> iterator = toDelete.iterator(); iterator.hasNext();) {
			String delete = iterator.next();
			entityURLs.remove(delete.toString());
			
		}
		return entityURLs;
	}
	
	/**
	 * @param ontClassUri
	 * @return
	 */
	private HashSet<String> listEntities(String ontClassUri) {
		HashSet<String> subjects = new HashSet<String>();
		ExtendedIterator individuals = this.ontology.listIndividuals();
		while (individuals.hasNext()) {
			Individual individual = (Individual) individuals.next();
			if (!individual.getURI().startsWith(this.ns)) continue;
		    if (individual.hasOntClass(ontClassUri)) {
		    	subjects.add(individual.getURI());
		    }
		}
		return subjects;
	}

	/**
	 * @param file
	 * @throws Exception
	 */
	public static Model loadDump(String file) throws Exception {
		Model dataset = FileManager.get().loadModel(file);
		return dataset;
	}

	/**
	 * @param file
	 * @throws Exception
	 */
	private void loadTdb(String file, int loadedDatasets) {
		if (loadedDatasets == 1) {
			throw new IllegalArgumentException("Please provide only one TDB folder (" + file + ").");
		}
		String tbdDirectory = file;
		Dataset ds = TDBFactory.createDataset(tbdDirectory) ;
		this.dataset = ds.getDefaultModel();
	}

	/**
	 * @param csvFileToRead
	 * @return
	 */
	private void readCsv(String csvFileToRead) {
		String line = "";
		String splitBy = ",";

		try {
			BufferedReader br = new BufferedReader(new FileReader(csvFileToRead));
			while ((line = br.readLine()) != null) {
				String[] csvLine = line.split(splitBy);
				if (csvLine.length > 1) {
					if (!this.alreadyProfiledClasses.contains(csvLine[0])) {
						this.alreadyProfiledClasses.add(csvLine[0]);
					}
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			new LODUCC(args, JENA);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
