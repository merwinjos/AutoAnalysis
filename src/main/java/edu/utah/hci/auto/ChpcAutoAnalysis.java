package edu.utah.hci.auto;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 1) Uses ssh to look for job directories with a RUNME
 * 2) Uses rsync to transfer over the files
 * 3) Reads the RUNME and copies over the workflow docs
 * 7) Executes using slurm
 * 7) Checks running and terminated jobs for COMPLETE, FAIL
 * 8) Rsyncs back completed jobs
 * 9) Uses sendmail to alert admin of any errors or app termination
 * Run on redwood2.chpc.utah.edu
 * */
public class ChpcAutoAnalysis {

	//needed config fields
	private File configFile = null;
	private String hciLinkDirectory = null;
	private File chpcJobDirectory = null;
	private boolean verbose = false;
	private double hoursToWait = 0;
	private long waitTime = 0;
	private String adminEmail = null;
	private int maxProcessingThreads;
	private String hciUserNameIp = null;
	private boolean dryRun = false;
	private File chpcTempDirectory = null;
	
	//internal fields
	private String slurmUserTruncated = null;
	private String slurmPartiton = null;
	private int numberAvailableNodes = 30;
	private File[] currentChpcJobDirs = null;
	private HashMap<String, String> currentSlurmJobIdTime = new HashMap<String, String>();
	private int currentRunningHciSlurmJobs = 0;
	private ArrayList<String> errorMessages = new ArrayList<String>();
	private ArrayList<File> chpcJobDirsToReturn = new ArrayList<File>();
	private ArrayList<String> hciJobDirsToCpToChpc = new ArrayList<String>();
	private ArrayList<String[]> commandsToExecute = new ArrayList<String[]>();
	private int numberRetries = 2;
	private CommandRunner[] commandRunners = null;
	private String printPrepend = null;
	private Random random = new Random();
	private double hoursPassed = 0;
	private int jobsProcessed = 0;
	
	public ChpcAutoAnalysis (String[] args) {
		try {

			processArgs(args);

			while (true) {
				Util.pl("\n########### "+ Util.getDateTime()+ " ###########");
				errorMessages.clear();
				
				// Check the running slurm jobs
				checkSlurmQueue();
				
				// Check the status of job directories on CHPC
				checkJobDirsOnChpc();
				
				// Any completed jobs?
				if (chpcJobDirsToReturn.size()!=0) {
					// Delete the contents of the linked job dirs on HCI
					deleteHCICompletedJobs();
					// Copy back completed jobs from CHPC to HCI
					copyBackCompletedJobs();
				}
				
				// Check for new jobs at HCI
				checkJobDirsOnHci();

				// Copy over new jobs and submit them to the slurm cluster
				if (hciJobDirsToCpToChpc.size() !=0) {
					copyJobDirsOnHci2Chpc();
					launchNewJobs();
				}

				// Email error messages? 
				emailErrorMessages();

				// Loop or exit?
				if (waitTime == 0) return;
				Util.pl(printPrepend+ "Sleeping "+hoursToWait+" hrs...");
				Thread.sleep(waitTime);
				
				emailAlive();
				
			}

		} catch (Exception e) {
			errorMessages.add("FATAL: ChpcAutoAnalysis terminated, daemon offline! Check CHPC log.");
			emailErrorMessages();
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
			String subject = "CHPC AutoAnalysis is alive "+Util.getDateTime();
			String body = "\n"+jobsProcessed+" jobs processed in the last 24hrs\n";
			Util.sendEmail(subject, adminEmail, body);
			jobsProcessed = 0;
		}
	}

	
	
	private void emailErrorMessages() {
		if (errorMessages.size()==0) return;
		Util.pl(printPrepend+ "Emailing error messages...");
		String subject = "ChpcAutoAnalysis ERROR";
		String body = Util.arrayListToString(errorMessages, "\n");
		Util.sendEmail(subject, adminEmail, body);
	}

	private void launchNewJobs() throws Exception {
		Util.pl(printPrepend+ "Launching new jobs...");
		
		ArrayList<File> shToExecute = new ArrayList<File>();
		
		//copy in workflow docs using the path in the RUNME file
		for (String jobDirName: hciJobDirsToCpToChpc) {
			File newJobDir = new File (chpcJobDirectory, jobDirName);
			File runme = new File (newJobDir, "RUNME");
			if (runme.exists() == false) throw new Exception("ERROR: failed to find "+runme);
			HashMap<String, String> keyValues = Util.loadFileIntoHash(runme, 0, 1);
			if (keyValues.containsKey("workflowPaths") == false) throw new Exception("ERROR: failed to find the 'workflowPaths' key in "+runme);
			String[] paths = Util.COMMA_SPACE.split(keyValues.get("workflowPaths").trim());
			ArrayList<File> toCopyIn = new ArrayList<File>();
			for (String p: paths) {
				File f = new File(p);
				if (f.exists() == false) throw new Exception("ERROR: failed to find "+f+ " as specified in "+runme);
				if (f.isFile()) toCopyIn.add(f);
				else {
					File[] toAdd = Util.extractFiles(f);
					for (File ta: toAdd) toCopyIn.add(ta);
				}
			}
			File[] toCopy = new File[toCopyIn.size()];
			toCopyIn.toArray(toCopy);
			if (toCopy.length == 0) throw new Exception("ERROR: failed to find any workflow doc files in "+runme);
			File shellScript = Util.copyInWorkflowDocs(toCopy, newJobDir);
			shToExecute.add(shellScript);
		}
		
		//create the cmds to execute in a shell script
		commandsToExecute.clear();
		for (File shellScript: shToExecute) {
			File newJobDir = shellScript.getParentFile();
			StringBuilder sb = new StringBuilder();
			// set to exit upon fail
			sb.append("set -e\n");
			// change into the job dir
			sb.append("cd "+newJobDir.getCanonicalPath()+"\n");
			// sbatch the shell script
			sb.append("sbatch --nice=10000 -J "+newJobDir.getName()+"_AutoAnalysis "+shellScript.getName()+ "\n");
			// touch QUEUED, needed if there are too many jobs and this goes into the slurm queue
			sb.append("touch QUEUED\n");
			
			commandsToExecute.add(new String[] {sb.toString()});
		}	
		
		//execute the cmds.
		if (dryRun) for (String[] c: commandsToExecute) Util.pl("\tDryRunExec\t"+Util.stringArrayToString(c, " "));
		else if (executeCommands(chpcTempDirectory) == false) throw new Exception("ERROR: copying new jobs from HCI to CHPC, aborting.");
	}

	private void copyJobDirsOnHci2Chpc() throws Exception {
		Util.pl(printPrepend+ "Copying new jobs from HCI to CHPC...");
		
		//create the cmds
		commandsToExecute.clear();
		for (String jobDirName: hciJobDirsToCpToChpc) {
			String[] cmd = {"rsync", "-rLt", "--size-only", hciUserNameIp+":"+hciLinkDirectory+jobDirName+"/",
					chpcJobDirectory.getCanonicalPath()+ "/"+ jobDirName+"/"};
			commandsToExecute.add(cmd);
		}
		//execute the cmds.
		if (dryRun) for (String[] c: commandsToExecute) Util.pl("\tDryRunExec\t"+Util.stringArrayToString(c, " "));
		else if (executeCommands(null) == false) throw new Exception("ERROR: copying new jobs from HCI to CHPC, aborting.");
	}

	private void checkJobDirsOnHci() throws Exception {
		Util.pl(printPrepend+ "Checking for new jobs on HCI...");
		hciJobDirsToCpToChpc.clear();
		commandsToExecute.clear();
		String[] cmd = {"ssh", hciUserNameIp, "find", "-L", hciLinkDirectory+"*","-maxdepth","1", "||", "true"};
		commandsToExecute.add(cmd);

		//execute the cmds.
		if (dryRun) for (String[] c: commandsToExecute) {
			Util.pl("\tDryRunExec\t"+Util.stringArrayToString(c, " "));
			return;
		}
		else if (executeCommands(null) == false) throw new Exception("ERROR: listing contents of the HCI job dir, aborting.");

		//parse the results
		HashSet<String> complete = new HashSet<String>();
		ArrayList<String> runme = new ArrayList<String>();

		for (String l: commandRunners[0].getProcessOutput()) {
			l=l.trim();
			if (l.endsWith("COMPLETE")) complete.add(l);
			else if (l.endsWith("RUNME")) runme.add(l);
		}

		//for each RUNME, see if there is a complete, if not then this is a new job ready to rsync to chpc
		for (String path: runme) {
			String comp = path.substring(0, path.length()-5)+"COMPLETE";
			if (complete.contains(comp) == false) {
				// at HCI : /home/tomatosrvs/NixAutoAlign/Test/AutoAlignJobs4CHPC/22597X4/RUNME
				if (verbose) Util.pl("\tHCIJob\t"+path);

				// already present at CHPC?
				String[] splitPath = Util.FORWARD_SLASH.split(path);
				String jobDirName = splitPath[splitPath.length-2];

				File chpcJobDir = new File(chpcJobDirectory, jobDirName);
				if (chpcJobDir.exists()) {
					if (verbose) Util.pl("\t\tAlready exists skipping "+jobDirName);
				}
				else {
					if (verbose) Util.pl("\t\tNew Job for transfer to CHPC "+jobDirName);
					hciJobDirsToCpToChpc.add(jobDirName);
				}
			}
		}
	}

	private void copyBackCompletedJobs() throws Exception {
		Util.pl(printPrepend+ "Copying back completed jobs from CHPC to HCI...");
		//create the cmds
		commandsToExecute.clear();
		for (File job: chpcJobDirsToReturn) {
			String[] cmd = {"rsync", "-rt", "--size-only", job.getCanonicalPath()+"/", hciUserNameIp+":"+hciLinkDirectory+job.getName()+"/"};
			commandsToExecute.add(cmd);
		}
		//execute the cmds.
		if (dryRun) for (String[] c: commandsToExecute) Util.pl("\tDryRunExec\t"+Util.stringArrayToString(c, " "));
		else if (executeCommands(null) == false) throw new Exception("ERROR: copying completed jobs from CHPC to HCI, aborting.");
		else {
			//delete jobs from CHPC so these aren't copied back again
			Util.pl(printPrepend+ "Deleting completed jobs at CHPC...");
			for (File job: chpcJobDirsToReturn) {
				if (verbose) Util.pl("\t"+job);
				Util.deleteDirectory(job);
			}
		}
		//record number of jobs completed
		jobsProcessed+= chpcJobDirsToReturn.size();
	}
	
	private void deleteHCICompletedJobs() throws Exception {
		Util.pl(printPrepend+ "Deleting the contents of the completed jobs on HCI...");
		
		// Must group these, the server can only take a dozen or so ssh calls in a minute before it starts to reject the calls and locks up for minutes.
		// Use something like ->  cat delme.sh | ssh tomatosrvs@hci-bio4.hci.utah.edu /bin/bash

		StringBuilder sb = new StringBuilder();
		for (File job: chpcJobDirsToReturn) {
			sb.append("rm -rf ");
			sb.append(hciLinkDirectory+job.getName());
			sb.append("/*\n");
		}

		//Make shell file with deletion commands
		File toDelete = new File (chpcTempDirectory, "tempFile_"+ random.nextInt(1000000) +".sh");
		Util.writeString(sb.toString(), toDelete);
		String[] cmd = {"cat", toDelete.getCanonicalPath(), "|", "ssh", hciUserNameIp, "/bin/bash"};

		//Execute in a shell script, java process builder doesn't handles pipes! This is a bash cmd line thing!
		CommandRunner runme = new CommandRunner(2, verbose, chpcTempDirectory, cmd);
		if (runme.isFailed()) throw new Exception("ERROR: deleting contents of HCI jobs, aborting.");
		toDelete.delete();
	}
	
	private boolean executeCommands(File tempShellScriptDir) {
		int numThreads = maxProcessingThreads;
		if (commandsToExecute.size() < numThreads) numThreads = commandsToExecute.size();
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		commandRunners = new CommandRunner[numThreads];
		for (int i=0; i< numThreads; i++) {
			commandRunners[i] = new CommandRunner(i,numberRetries, this, verbose, tempShellScriptDir);
			executor.execute(commandRunners[i]);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {}
		
		//check the runners for errors
		boolean completedOK = true;
		for (CommandRunner cr: commandRunners) {
			if (cr.isFailed()) {
				completedOK = false;
				Util.pl(cr.getErrorMessage());
			}
		}
		return completedOK;
		
	}

	public synchronized String[] getNextCommandToRun() {
		if (commandsToExecute.size() == 0) return null;
		String[] next = commandsToExecute.remove(0);
		return next;
	}

	private void checkJobDirsOnChpc() throws Exception {
		Util.pl(printPrepend+ "Checking CHPC job directories for status messages...");
		currentChpcJobDirs = Util.extractOnlyDirectories(chpcJobDirectory);
		chpcJobDirsToReturn.clear();
		//check if no job dirs
		if (currentChpcJobDirs == null) return;
		
		// for each dir pull the files and check the status, 
		// upon sbatching a QUEUED file is added to the job dir
		// upon job start by slurm: 'rm -f FAILED COMPLETE QUEUED; touch STARTED'
		
		HashMap<String, File> fileNames = null;
		for (File jobDir: currentChpcJobDirs) {
			fileNames = Util.fetchNamesAndFiles(jobDir);
			
			//ready for transfer back?
			if (fileNames.containsKey("COMPLETE")) {
				chpcJobDirsToReturn.add(jobDir);
				Util.pl("\tCOMPLETE ->\t"+jobDir);
			}
			
			//failed job add to error messages and leave in place
			else if (fileNames.containsKey("FAILED")) {
				String error = "FAILED ->\t"+jobDir;
				errorMessages.add(error);
				Util.pl("\t"+error);
			}
			
			//job started, check in slurm list of jobs
			else if (fileNames.containsKey("STARTED")) {
				//find the slurm-6485303.out file(s) should be just one!
				File[] slurms = Util.extractFilesPrefix(jobDir, "slurm-");
				String error = null;
				if (slurms.length == 0) error = "ERROR: job STARTED but failed to find a slurm-xxx.out file, see -> "+jobDir;
				else if (slurms.length > 1) error = "ERROR: more than one slurm-xxx.out files in -> "+jobDir;
				else {
					//parse the ID
					Matcher mat = Util.SLURM_JOBID.matcher(slurms[0].getName());
					if (mat.matches()) {
						String id = mat.group(1);
						if (currentSlurmJobIdTime.containsKey(id) == false) {
							error = "ERROR: job STARTED, found "+slurms[0].getName()+" file but JOBID is not in slurm queue , see -> "+jobDir;
						}
						//OK its still running
						else Util.pl("\tRUNNING ->\t"+ currentSlurmJobIdTime.get(id)+"\t"+jobDir);
					}
					else throw new Exception("ERROR pulling the slurm job ID from "+slurms[0].getName());
				}
				if (error != null) {
					errorMessages.add(error);
					Util.pl("\t"+error);
				}
			}
			
			//submitted to queue, but job not launched, check number of available jobs
			else if (fileNames.containsKey("QUEUED")) {
				if (currentRunningHciSlurmJobs < numberAvailableNodes) {
					String error = "WARNING: job is QUEUED but failed to start yet nodes are available, see -> "+jobDir;
					errorMessages.add(error);
					Util.pl("\t"+error);
				}
				else if (verbose) Util.pl("\tQUEUED ->\t"+jobDir);
			}
			
			//something is wrong, no status message
			else {
				String error = "ERROR: no job status message, see -> "+jobDir;
				errorMessages.add(error);
				Util.pl("\t"+error);
			}
		}
		
	}

	private void checkSlurmQueue() throws IOException {
		Util.pl(printPrepend+ "Checking slurm jobs...");
		//pull all of the jobs in the queue
		String[] cmd = {"squeue"};
		String[] results = Util.executeCommandLine(cmd);
		
		//Does it show the expected?
		if (results[0].trim().startsWith("JOBID") == false) throw new IOException("\nERROR: failed to fetch slurm jobs:\n "+Util.stringArrayToString(results, "\n"));
		currentSlurmJobIdTime.clear();
		
		for (int i=1; i< results.length; i++) {
			results[i] = results[i].trim();
			String[] fields = Util.WHITE_SPACE.split(results[i]);
			//JOBID	   PARTITION	NAME	  USER	   ST	TIME	NODES	NODELIST(REASON)
			//6485242	hci-rw	  testjob.	hcipepip	R	0:10	1	    rw166
			//                                          PD for in queue
			//   0         1          2         3       4     5     6         7
			
			if (fields[1].equals(slurmPartiton)) {
				currentRunningHciSlurmJobs++;
				if (fields[3].equals(slurmUserTruncated)) {
					currentSlurmJobIdTime.put(fields[0], fields[5]);
					//too long? In days-hours:minutes:seconds.  The days and hours are printed only as needed. If - present then more than a 24hrs have passed, flag it.
					if (fields[5].contains("-")) {
						String error = "WARNING: the following job has run for more than a day ->\t"+fields[0]+"\t"+fields[5];
						Util.pl("\t"+error);
						errorMessages.add(error);
					}
				}
			}
		}
		
		if (verbose) Util.pl("\tParsed Slurm Jobs (ID=RunTime): "+currentSlurmJobIdTime);
	}

	public static void main(String[] args) {
		if (args.length <= 2){
			printDocs();
			System.exit(0);
		}
		new ChpcAutoAnalysis(args);
	}		


	/**This method will process each argument and assign new variables*/
	public void processArgs(String[] args){
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
					case 'd': dryRun = true; break;
					case 'v': verbose = true; break;
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
		
		if (verbose) printPrepend = "\n";
		else printPrepend = "";
	}	

	private void loadConfiguration() {
		HashMap<String,String> configSettings = Util.loadFileIntoHash(configFile, 0, 1);

		//adminEmail
		adminEmail = configSettings.get("adminEmail");
		if (adminEmail == null) Util.printErrAndExit("\nError: failed to find the 'adminEmail' key in "+ configFile);
		
		//HCI link directory, check it ends with /
		if (configSettings.containsKey("hciLinkDirectory") == false) Util.printErrAndExit("\nError: failed to find the 'hciLinkDirectory' key in "+ configFile);
		hciLinkDirectory = configSettings.get("hciLinkDirectory");
		if (hciLinkDirectory.endsWith("/") == false) hciLinkDirectory = hciLinkDirectory+"/";
		
		
		//CHPC slurm job directory
		if (configSettings.containsKey("chpcJobDirectory") == false) Util.printErrAndExit("\nError: failed to find the 'chpcJobDirectory' key in "+ configFile);
		chpcJobDirectory = new File(configSettings.get("chpcJobDirectory"));	
		if (chpcJobDirectory.canWrite() == false) Util.printErrAndExit("\nError: cannot write to the 'chpcJobDirectory'"+ chpcJobDirectory);

		//CHPC temporary shell script directory
		if (configSettings.containsKey("chpcTempDirectory") == false) Util.printErrAndExit("\nError: failed to find the 'chpcTempDirectory' key in "+ configFile);
		chpcTempDirectory = new File(configSettings.get("chpcTempDirectory"));	
		if (chpcTempDirectory.canWrite() == false) Util.printErrAndExit("\nError: cannot write to the 'chpcTempDirectory'"+ chpcTempDirectory);
		
		//slurmUserTruncated for sbatch slurm checking
		if (configSettings.containsKey("slurmUserTruncated") == false) Util.printErrAndExit("\nError: failed to find the 'slurmUserTruncated' key in "+ configFile);
		slurmUserTruncated = configSettings.get("slurmUserTruncated");
		
		//slurmPartiton for parsing the sbatch output
		if (configSettings.containsKey("slurmPartiton") == false) Util.printErrAndExit("\nError: failed to find the 'slurmPartiton' key in "+ configFile);
		slurmPartiton = configSettings.get("slurmPartiton");
		
		//Rsync threads
		if (configSettings.containsKey("maxProcessingThreads") == false) Util.printErrAndExit("\nError: failed to find the 'maxProcessingThreads' key in "+ configFile);
		maxProcessingThreads = Integer.parseInt(configSettings.get("maxProcessingThreads"));

		//HoursToWait
		if (configSettings.containsKey("hoursToWait") == false) Util.printErrAndExit("\nError: failed to find the 'hoursToWait' key in "+ configFile);
		hoursToWait = Double.parseDouble(configSettings.get("hoursToWait"));
		waitTime = (long)Math.round(hoursToWait * 60.0 * 60.0 * 1000.0);
		
		//hciUserNameIp for ssh and rsync calls from CHPC to HCI
		if (configSettings.containsKey("hciUserNameIp") == false) Util.printErrAndExit("\nError: failed to find the 'hciUserNameIp' key in "+ configFile);
		hciUserNameIp = configSettings.get("hciUserNameIp");
		
		
		//print out settings
		Util.pl("Config Settings..."+
				"\n  adminEmail\t"+ adminEmail+
				"\n  hoursToWait\t"+ hoursToWait+
				"\n  verbose\t"+verbose+
				"\n  dryRun\t"+dryRun+
				
				"\n\nCHPC:"+
				"\n  chpcJobDirectory\t"+ chpcJobDirectory+
				"\n  chpcTempDirectory\t"+ chpcTempDirectory+
				"\n  slurmUserTruncated\t"+ slurmUserTruncated+
				"\n  slurmPartiton\t"+ slurmPartiton+

				"\n\nHCI:"+
				"\n  hciUserNameIp\t"+ hciUserNameIp+
				"\n  hciLinkDirectory\t"+ hciLinkDirectory
				);
	}



	public static void printDocs(){
		Util.pl("\n" +
				"**************************************************************************************\n" +
				"**                            CHPC Auto Analysis: Jan 2024                          **\n" +
				"**************************************************************************************\n" +
				"CAA orchestrates auto analysis of fastq files in GNomEx Experiment Requests. \n"+

				"\nOptions:\n"+
				"   -c Path to the CAA configuration file.\n"+
				"   -v Verbose debugging output.\n"+
				"   -d Dry runs, just print ssh and rsync cmds.\n"+
				

				"\nExample: java -jar ChpcAutoAnalysis.jar -c autoAnalysisConfig.txt\n\n"+


				"**************************************************************************************\n");

	}
}



