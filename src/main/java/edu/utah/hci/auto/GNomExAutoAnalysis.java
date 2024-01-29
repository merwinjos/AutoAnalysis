package edu.utah.hci.auto;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**Sets up primary AutoAnalysis folders for GNomEx Experiment Requests
 * 1) Interrogates the standard GNomEx sql server for analysis requests submitted with the Experiment Request
 * 2) Looks in the GNomEx repo to see which have been completed, are in process, or have failed
 * 3) Runs MultiQC on those that are all complete
 * 7) Emails clients regarding the job status.
 * Run on hci-deadhorse.hci.utah.edu
 * */
public class GNomExAutoAnalysis {

	//config fields
	private File configFile = null;
	private File supportedOrgLibWfConfigFile = null;
	private String connectionUrl = null;
	private HashMap<String, File> experimentalSubDirs = null;
	private String adminEmail = null;
	private String hciLinkDirectoryString = null;
	private File hciLinkDirectory = null;
	private boolean verbose = false;
	private double hoursToWait = 0;
	private long waitTime = 0;
	private File useqJobCleaner = null;
	private String experimentLinkUrl = null;
	private String jiraUrl = null;
	private String dataPolicyUrl = null;
	private String experimentRequestsToProc = null;
	
	//internal fields
	//Date formatting, 2023-11-14 07:43:13.38
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private Calendar calendar = Calendar.getInstance();
	private int numberRetriesForMultiQC = 2;
	private HashMap<String, String[]> orgLibWorkflowDocs = null;
	private double hoursPassed = 0;
	private int jobsProcessed = 0;
	
	//Requests split by status
	private ArrayList<GNomExRequest> grsToBuildAutoAnalysis = new ArrayList<GNomExRequest>();
	private ArrayList<GNomExRequest> grsWithAutoAnalysis = new ArrayList<GNomExRequest>();
	private ArrayList<GNomExRequest> grsSkipped = new ArrayList<GNomExRequest>();
	private ArrayList<GNomExRequest> grsOtherHelpRequests = new ArrayList<GNomExRequest>();
	private ArrayList<GNomExRequest> grsToMultiQC = new ArrayList<GNomExRequest>();
	private ArrayList<String> errorMessages = new ArrayList<String>();

	public GNomExAutoAnalysis (String[] args) {
		try {

			processArgs(args);

			while (true) {
				Util.pl("\n########### "+ Util.getDateTime()+ " ###########");
				
				// Query the GNomEx DB for experiment requests
				Util.pl("\nChecking the GNomEx db...");
				GNomExDbQuery dbQuery = new GNomExDbQuery(connectionUrl, verbose);
				if (dbQuery.isFailed()) throw new Exception("ERROR with querying the GNomEx DB");

				// Find new requests ready for analysis, find existing analysis jobs and check their status
				parseRequests(dbQuery.getRequests());

				// Build new AutoAnalysis Jobs
				buildAutoAnalysisJobs();

				// Check the existing AutoAnalysis (AutoAnalysis/22597R_27Dec2023) and it's sub job directories (AutoAnalysis/22597R_27Dec2023/22597X4)
				checkExistingAutoAnalysis();

				// Run MultiQC and delete the symlinked AutoAnalysis jobs
				runMultiQCEmailClients();

				// Loop or exit?
				if (hoursToWait == 0) return;
				Util.pl("Sleeping "+hoursToWait+" hrs ...");
				Thread.sleep(waitTime);
				
				clearPriorArrays();
				
				emailAlive();

			}

		} catch (Exception e) {
			emailErrorMessage("FATAL: GNomExAutoAnalysis terminated, daemon offline! Check HCI run log.\n", e);
			e.printStackTrace();
			System.exit(1);
		}
	}

	/*Sends an email that service is alive every 24hrs*/
	private void emailAlive() {
		hoursPassed += hoursToWait;
		if (hoursPassed >= 24) {
			hoursPassed = 0;
			Util.pl("Emailing admin that daemon is running...");
			String subject = "GNomEx AutoAnalysis is alive "+Util.getDateTime();
			String body = "\n"+jobsProcessed+" jobs processed in the last 24hrs\n";
			Util.sendEmail(subject, adminEmail, body);
			jobsProcessed = 0;
		}
		
	}

	private void clearPriorArrays() {
		grsToBuildAutoAnalysis.clear();
		grsWithAutoAnalysis.clear();
		grsSkipped.clear();
		grsOtherHelpRequests.clear();
		grsToMultiQC.clear();
		errorMessages.clear();
	}

	private void emailErrorMessage(String error, Exception e) {
		Util.pl("Emailing error messages...");
		String subject = "GNomExAutoAnalysis ERROR";
		String body = error+"\n"+e.toString();
		Util.sendEmail(subject, adminEmail, body);
	}


	private void runMultiQCEmailClients() throws IOException {
		// Any jobs?
		if (grsToMultiQC.size() ==0) return;
		
		
		Util.pl("\nRunning MultiQC and JobCleaner...");
		for (GNomExRequest gr: grsToMultiQC) {
			
			// AutoAnalysis_22Dec2023
			if (verbose) Util.pl("\t"+gr.getAutoAnalysisMainDirectory());
			String alignDir = gr.getAutoAnalysisMainDirectory().getCanonicalPath();
			String jobsDir = gr.getAutoAnalysisJobsDirectory().getCanonicalPath();
			String name = gr.getRequestIdCleaned();
			
			// See if there are any additional multiQC options
			String orgLib = gr.getOrganism()+"_"+gr.getLibraryPreparation();
			String opts = orgLibWorkflowDocs.get(orgLib)[1].trim();
			if (opts.toLowerCase().equals("none")) opts = "";
			
			StringBuilder sb = new StringBuilder();
			// set to exit upon fail
			sb.append("set -e\n");
			// docker multiqc
			sb.append("docker run --rm --user $(id -u):$(id -g) -v "+alignDir+":"+alignDir+
					" ewels/multiqc multiqc "+opts+" --outdir "+alignDir+"/MultiQC --title "+name+
					" --filename "+name+"_MultiQCReport.html "+jobsDir+"\n");
			// small file cleanup
			sb.append("java -jar -Xmx5G "+useqJobCleaner.getCanonicalPath()+" -n 'Logs,RunScripts' -m -r "+
					jobsDir+" -e 'COMPLETE,.tbi,.crai,.bai'\n");
			
			//run it, will retry
			CommandRunner cr = new CommandRunner (numberRetriesForMultiQC, verbose, gr.getAutoAnalysisMainDirectory(), new String[] {sb.toString()});
			if (cr.isFailed()) throw new IOException(cr.getErrorMessage());
			
			//email client that data is ready
			emailClient(gr);
		}
	}

	private void emailClient(GNomExRequest gr) {
		String subject = "GNomEx AutoAnalysis for "+gr.getRequestIdCleaned()+" is complete";
		
		StringBuilder sb = new StringBuilder();
		sb.append("AutoAnalysis and MultiQC have completed for the samples in "+gr.getRequestIdCleaned()+ "\n\n");
		sb.append("Access these via:\n\t"+experimentLinkUrl+ gr.getOriginalRequestId()+ "\n\tand look in the  '"+gr.getAutoAnalysisMainDirectory().getName()+"'  directory.\n\n");
		sb.append("If you would like additional analysis assistance, submit a help request through our Jira ticketing system:\n\t"+ jiraUrl+"\n\n");
		sb.append("Lastly, remember that all of the data files in this Experiment Request will be deleted after six months. If your group has a CORE "
				+ "Browser configured AWS account, the files will be uploaded into it and then deleted. See:\n\t"+ dataPolicyUrl+ "\n\n");
		sb.append("HCI Cancer Bioinformatics Shared Resource (CBI)\nhttps://huntsmancancer.org/cbi\n\n");

Util.pl("\tEmailing ADMIN! Change back to client!");
Util.sendEmail(subject, adminEmail, sb.toString());
//Util.sendEmail(subject, gr.getRequestorEmail(), sb.toString());
		
	}

	private void checkExistingAutoAnalysis() throws Exception{
		// Any jobs?
		if (grsWithAutoAnalysis.size() ==0) return;
		
		Util.pl("\nChecking AutoAnalysis jobs...");
		for (GNomExRequest gr: grsWithAutoAnalysis) {
			
			//Is it all complete? e.g. MultiQC has run and the symlinked dirs are removed
			File complete = new File (gr.getAutoAnalysisMainDirectory(), "COMPLETE");
			File mqc = new File (gr.getAutoAnalysisMainDirectory(), "MultiQC");
			if (complete.exists() || mqc.exists()) {
				if (verbose) Util.pl("\tCOMPLETE\t"+ gr.getAutoAnalysisMainDirectory());
				continue;
			}
			
			
			//OK, check sub directories
			if (verbose) Util.pl("\t"+ gr.getAutoAnalysisJobsDirectory());
			File[] jobs = Util.extractOnlyDirectories(gr.getAutoAnalysisJobsDirectory());
			
			boolean allComplete = true;
			for (File jobDir: jobs) {
				File comp = new File(jobDir, "COMPLETE");
				//the only jobs copied back will have a COMPLETE, otherwise they are waiting on CHPC
				if (comp.exists() == false) {
					allComplete = false; 
					if (verbose) Util.pl("\t\tWAITING ON\t"+jobDir.getName());
				}
				else if (verbose) Util.pl("\t\tCOMPLETE\t"+jobDir.getName());
			}
			
			//setup for multi qc?
			if (allComplete) {
				grsToMultiQC.add(gr);
				for (File jobDir: jobs) {
					String symLinkName = hciLinkDirectoryString+jobDir.getName();
					Path sl = Paths.get(symLinkName);
					if(Files.exists(sl)) {
						//Delete symlinked job dirs
						if (verbose) Util.pl("\t\tDeleting symlinked job dir\t"+symLinkName);
						Files.delete(sl);
					}
				}
				//record how many jobs processed
				jobsProcessed+= jobs.length;
			}
		}
	}

	private void buildAutoAnalysisJobs() throws IOException {
		// Any jobs?
		if (grsToBuildAutoAnalysis.size() ==0) return;
		Util.pl("\nBuilding new AutoAnalysis jobs...");
		
		for (GNomExRequest r: grsToBuildAutoAnalysis) {
			boolean created = r.createAutoAnalysisJobs(hciLinkDirectory);
			if (created == false) throw new IOException("Failed to create a AutoAnalysis job for "+r.getRequestIdCleaned());
		}
	}

	private void parseRequests(GNomExRequest[] requests) throws Exception {
		Util.pl("\nParsing GNomExRequests...");

		boolean test = experimentRequestsToProc.toLowerCase().equals("all") == false;
			
		for (GNomExRequest r: requests) {

			if (test) {
				if (r.getRequestIdCleaned().equals(experimentRequestsToProc) == false) continue;
				else Util.pl("\nTest ExperimentRequest:\n"+r+"\n");
			}
			else if (verbose) Util.pl("\tSummary:\t"+r.simpleToString());
			
			//do they want alignment and qc?
			if (r.getGenomeBuild().equals("NA")) {
				grsOtherHelpRequests.add(r);
				r.setErrorMessages("No genome build selected, skipping AutoAnalysis");
				if (verbose) Util.pl("\t"+ r.getErrorMessages());
				continue;
			}

			//check for the presence of an AutoAnalysis folder and a Fastq folder
			//find the appropriate year for the request
			calendar.setTime(dateFormat.parse(r.getCreationDate()));
			String year = Integer.toString(calendar.get(Calendar.YEAR));
			File repoYearSubDir = experimentalSubDirs.get(year);
			if (repoYearSubDir == null) throw new IOException ("Failed to find the year "+year+" sub directory in "+experimentalSubDirs +" for "+r.getRequestIdCleaned());
			File requestDirOnRepo = new File (repoYearSubDir, r.getRequestIdCleaned());
			if (requestDirOnRepo.exists() == false) {
				Util.pl("\tERROR: failed to find the Request directory "+requestDirOnRepo+" skipping!");
				r.setErrorMessages("Failed to find the request directory in the repo : "+ requestDirOnRepo);
				grsSkipped.add(r);
			}
			else {
				r.setRequestDirectory(requestDirOnRepo);
				//check for AutoAnalysis
				if (r.checkForAutoAnalysis()) {
					if (verbose) Util.pl("\tFound exiting AutoAnalysis dir");
					grsWithAutoAnalysis.add(r);
					continue;
				}
				//check fastq if AutoAnalysis not found
				if (r.checkFastq() == false) {
					if (verbose) Util.pl("\tFailed to find Fastq ready for AutoAnalysis in the Request directory "+requestDirOnRepo+" skipping! No md5? Too new?");
					r.setErrorMessages("Failed to find Fastq ready for AutoAnalysis in the repo : "+ requestDirOnRepo);
					grsSkipped.add(r);
					continue;
				}

				//run a bunch of other checks to see if an AutoAnalysis could be assembled
				else {
	
					//is this a supported organism_libraryPrep?
					String orgLib = r.getOrganism()+"_"+r.getLibraryPreparation();
					if (orgLibWorkflowDocs.containsKey(orgLib)) {
						if (verbose) Util.pl("\tReady for Bulk RNASeq AutoAnalysis");
						String[] pathsMultiQCOptions = orgLibWorkflowDocs.get(orgLib);
						r.setWorkflowPaths(pathsMultiQCOptions[0]);
						grsToBuildAutoAnalysis.add(r);
					}
					else {
						grsSkipped.add(r);
						r.setErrorMessages("Library Protocol not supported at this time, skipping AutoAnalysis ");
						if (verbose) Util.pl("\t"+ r.getErrorMessages());
					}
				}
			}
		}
		//stats
		Util.pl("\t"+grsToBuildAutoAnalysis.size()+ "\tAutoAnalysis to run");
		Util.pl("\t"+grsWithAutoAnalysis.size()+ "\tWith an existing AutoAnalysis");
		Util.pl("\t"+grsSkipped.size()+ "\tSkipped");
		Util.pl("\t"+grsOtherHelpRequests.size()+ "\tOthers with non AutoAnalysis help requests");
	}


	public static void main(String[] args) {
		if (args.length <= 2){
			printDocs();
			System.exit(0);
		}
		new GNomExAutoAnalysis(args);
	}		


	/**This method will process each argument and assign new variables
	 * @throws Exception */
	public void processArgs(String[] args) throws Exception{
		Util.pl("Arguments: "+ Util.stringArrayToString(args, " ") +"\n");
		Pattern pat = Pattern.compile("-[a-z]");
		for (int i = 0; i<args.length; i++){
			String lcArg = args[i].toLowerCase();
			Matcher mat = pat.matcher(lcArg);
			if (mat.matches()){
				char test = args[i].charAt(1);
				try{
					switch (test){
					case 'c': configFile = new File(args[++i]); break;
					case 'v': verbose = true; break;
					case 'd': ; break;
					case 'l': ; break;
					default: Util.printErrAndExit("\nProblem, unknown option! " + mat.group());
					}
				}
				catch (Exception e){
					Util.printErrAndExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
				}
			}
		}

		//pull config file, a simple key value file that skips anything with #, splits on tab
		if (configFile == null || configFile.canRead() == false) Util.printErrAndExit("Error: please provide a path to the AutoAnalysis configuration file.\n");

		//load and check the configSettings
		loadConfiguration();
		
		loadSupportedWorkflows();


	}	

	private void loadSupportedWorkflows() throws Exception {
		Util.pl("\nParsing Supported Organisms and Library Preps...");
		
		BufferedReader in = Util.fetchBufferedReader(supportedOrgLibWfConfigFile);
		String[] fields;
		String line;
		
		orgLibWorkflowDocs = new HashMap<String, String[]>();
		
		while ((line = in.readLine())!=null) {
			line = line.trim();
			if (line.startsWith("#") || line.length()==0) continue;
			//Organism LibraryKit WFDirFiles MultiQCOptions
			fields = Util.TAB.split(line);
			if (fields.length != 4) Util.pl("\tWARNING: missing fields, skipping -> "+line);
			else {
				String key = fields[0].trim()+"_"+fields[1].trim();
				orgLibWorkflowDocs.put(key, new String[] {fields[2], fields[3]});
				if (verbose) Util.pl("\t"+line);
			}
		}
		in.close();
		
	}

	private void loadConfiguration() {
		Util.pl("\nParsing Configuration File...");
		HashMap<String,String> configSettings = Util.loadFileIntoHash(configFile, 0, 1);

		//connectionUrl
		connectionUrl = configSettings.get("connectionUrl");
		if (connectionUrl == null) Util.printErrAndExit("\nError: failed to find the 'connectionUrl' key in "+ configFile);
		
		// experimentLinkUrl
		experimentLinkUrl = configSettings.get("experimentLinkUrl");
		if (experimentLinkUrl == null) Util.printErrAndExit("\nError: failed to find the 'experimentLinkUrl' key in "+ configFile);
		
		// jiraUrl
		jiraUrl = configSettings.get("jiraUrl");
		if (jiraUrl == null) Util.printErrAndExit("\nError: failed to find the 'jiraUrl' key in "+ configFile);
		
		// dataPolicyUrl
		dataPolicyUrl = configSettings.get("dataPolicyUrl");
		if (dataPolicyUrl == null) Util.printErrAndExit("\nError: failed to find the 'dataPolicyUrl' key in "+ configFile);
		
		//adminEmail
		adminEmail = configSettings.get("adminEmail");
		if (adminEmail == null) Util.printErrAndExit("\nError: failed to find the 'adminEmail' key in "+ configFile);
		
		//testRequest
		experimentRequestsToProc = configSettings.get("testRequest");
		if (experimentRequestsToProc == null) Util.printErrAndExit("\nError: failed to find the 'testRequest' key in "+ configFile);
		
		//HoursToWait
		if (configSettings.containsKey("hoursToWait") == false) Util.printErrAndExit("\nError: failed to find the 'hoursToWait' key in "+ configFile);
		hoursToWait = Double.parseDouble(configSettings.get("hoursToWait"));
		waitTime = (long)Math.round(hoursToWait * 60.0 * 60.0 * 1000.0);

		//experimental directories
		String experimentDirString = configSettings.get("experimentDir");
		if (experimentDirString == null) Util.printErrAndExit("\nError: failed to find the 'experimentDir' key in "+ configFile);
		File ed = new File(experimentDirString);
		if (ed.exists() == false) Util.printErrAndExit("\nError: failed to find the 'experimentDir' directory in "+ configFile);
		experimentalSubDirs = Util.extractDirectories(ed);
		if (experimentalSubDirs.containsKey("2023") == false) Util.printErrAndExit("\nError: failed to find the '2023' directory in "+ ed);

			
		//chpc
		if (configSettings.containsKey("hciLinkDirectory") == false) Util.printErrAndExit("\nError: failed to find the 'hciLinkDirectory' key in "+ configFile);
		hciLinkDirectoryString = configSettings.get("hciLinkDirectory");
		if (hciLinkDirectoryString.endsWith("/")==false) hciLinkDirectoryString = hciLinkDirectoryString+"/";
		hciLinkDirectory = new File (hciLinkDirectoryString);	
		if (hciLinkDirectory.canWrite() == false) Util.printErrAndExit("\nError: cannot write to the 'hciLinkDirectory'"+ hciLinkDirectory);
		
		//JobCleaner path command
		if (configSettings.containsKey("useqJobCleaner") == false) Util.printErrAndExit("\nError: failed to find the 'useqJobCleaner' key in "+ configFile);
		useqJobCleaner = new File (configSettings.get("useqJobCleaner"));
		if (useqJobCleaner.exists() == false) Util.printErrAndExit("\nError: failed to find the 'useqJobCleaner' app in "+ configFile);
		
		// supportedOrgLibWfConfigFile
		if (configSettings.containsKey("supportedOrgLibWfConfigFile") == false) Util.printErrAndExit("\nError: failed to find the 'supportedOrgLibWfConfigFile' key in "+ configFile);
		supportedOrgLibWfConfigFile = new File (configSettings.get("supportedOrgLibWfConfigFile"));
		if (supportedOrgLibWfConfigFile.exists() == false) Util.printErrAndExit("\nError: failed to find the 'supportedOrgLibWfConfigFile' app in "+ configFile);
		
		
		//print out settings
		Util.pl("Config Settings..."+
				"\n  adminEmail\t"+ adminEmail+
				"\n  hoursToWait\t"+ hoursToWait+
				"\n  verbose\t"+verbose+
				"\n  connectionUrl\tSee config file"+
				"\n  experimentDirString\t"+ experimentDirString+
				"\n  experimentLinkUrl\t"+ experimentLinkUrl+
				"\n  experimentRequestsToProc\t"+ experimentRequestsToProc+
				"\n  hciLinkDirectory\t"+ hciLinkDirectory+
				"\n  jiraUrl\t"+ jiraUrl+
				"\n  dataPolicyUrl\t"+ dataPolicyUrl+
				"\n  supportedOrgLibWfConfigFile\t"+ supportedOrgLibWfConfigFile+
				"\n  useqJobCleaner\t"+ useqJobCleaner
				);
		
	}


	public static void printDocs(){
		Util.pl("\n" +
				"**************************************************************************************\n" +
				"**                            GNomEx Auto Analysis: Jan 2024                        **\n" +
				"**************************************************************************************\n" +
				"GAA orchestrates setting up auto analysis jobs from GNomEx Experiment fastq. It looks\n"+
				"for ERs in the GNomEx db where the user has selected a genome build to align to,\n"+
				"finds those with a match to the supported organisms and library types, assembles the\n"+
				"directories, links in the fastq, and adds a RUNME file containing info for the\n "+
				"ChpcAutoAnalysis daemon.\n"+

				"\nOptions:\n"+
				"   -c Path to the AutoAnalysis configuration file.\n"+
				"   -v Verbose debugging output.\n"+

				"\nExample: java -jar GNomExAutoAnalysis.jar -c autoAnalysis.config.txt -v \n\n"+


				"**************************************************************************************\n");

	}


}



