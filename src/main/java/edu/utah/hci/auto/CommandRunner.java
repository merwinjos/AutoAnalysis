package edu.utah.hci.auto;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;


public class CommandRunner implements Runnable {
	
	//fields
	private int id = 0;
	private ChpcAutoAnalysis caa = null;
	private String[] command = null;
	private String commandString = null;
	private int numberRetries;
	private int exitCode = -1;
	private boolean failed = false;
	private boolean verbose = false;
	private File tempDirForShellScripts = null;
	private ArrayList<String> processOutput = new ArrayList<String>();
	private String errorMessage = null;
	private ArrayList<String[]> completedCommands = new ArrayList<String[]>();
	private File tempShellFile = null;
	private Random random = new Random();
	private static final long timeToWait = 1000*60*5;
	
	// for multiple threaded runners
	public CommandRunner (int id, int numberRetries, ChpcAutoAnalysis caa, boolean verbose, File tempDirForShellScripts) {
		this.id = id;
		this.caa = caa;
		this.numberRetries = numberRetries;
		this.verbose = verbose;
		this.tempDirForShellScripts = tempDirForShellScripts;
	}
	
	// for just one execution in the main thread
	public CommandRunner (int numberRetries, boolean verbose, File tempDirForShellScripts, String[] command) {
		try {
			this.numberRetries = numberRetries;
			this.verbose = verbose;
			this.tempDirForShellScripts = tempDirForShellScripts;
			this.command = command;
			
			//set the commandString
			commandString = Util.stringArrayToString(command, " ");
			if (verbose) Util.pl(id+" Executing:\n"+commandString);
			
			// use shell script? this will change the command to point to a temp file to run as a bash script
			if (tempDirForShellScripts !=null) setShellScriptCommand();

			//try multiple times until exit code is 0 or the retries all fail
			executeWithRetries();

		} catch (Exception e) {
			failed = true;
			errorMessage = id+" Error: problem running -> "+commandString+"\n"+processOutput+"\n"+e.getMessage();
			Util.pl("\n"+errorMessage );
			if (verbose) e.printStackTrace();
		} 
	}
	
	public void run() {	
		try {
			//get next command, once null no more to run
			while ((command = caa.getNextCommandToRun()) != null) {
				//set the commandString
				commandString = Util.stringArrayToString(command, " ");
				if (verbose) Util.pl(id+" Executing:\n"+commandString);
				processOutput.clear();
				exitCode = -1;
				failed = false;
				
				// use shell script?
				if (tempDirForShellScripts !=null) setShellScriptCommand();
				
				//try multiple times until exit code is 0
				executeWithRetries();
			}
			
		} catch (Exception e) {
			failed = true;
			errorMessage = id+" Error: problem running -> "+commandString+"\n"+processOutput+"\n"+e.getMessage()+"\nShutting down CommandRunner.";
			Util.pl(errorMessage);
			if (verbose) e.printStackTrace();
		} 
	}
	
	public void setShellScriptCommand () throws IOException {
		//make shell file
		tempShellFile = new File (tempDirForShellScripts, "tempFile_"+ random.nextInt(1000000) +".sh");
		//write to shell file
		Util.writeString(commandString, tempShellFile);
		tempShellFile.setExecutable(true);
		command = new String[]{"/usr/bin/bash", tempShellFile.getCanonicalPath()};
	}
	
	public void executeWithRetries() throws Exception {
		//try multiple times until exit code is 0
		int tries = numberRetries;
		while (tries-- >= 0) {
			execute();
			if (exitCode == 0) {
				completedCommands.add(command);
				if (verbose) Util.pl(id+" Complete:\n"+Util.arrayListToString(processOutput, ","));
				break;
			}
			if (verbose) Util.pl(id+" Waiting 5 min and retrying:");
			Thread.sleep(timeToWait);
		}
		
		//success?
		if (exitCode != 0) throw new Exception(id+" Failed to complete after "+(numberRetries+1)+" tries.");
		else if (tempShellFile != null) tempShellFile.delete();
	}
	
	/**Uses ProcessBuilder to execute a cmd line, combines standard error and standard out sets the exit code.
	 * @throws Exception */
	public void execute() throws Exception {
		ProcessBuilder pb = new ProcessBuilder(command);
		pb.redirectErrorStream(true);
		Process proc = pb.start();
		BufferedReader data = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line;
		while ((line = data.readLine()) != null) processOutput.add(line);
		data.close(); 
		exitCode = proc.waitFor();
	}

	public boolean isFailed() {
		return failed;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public ArrayList<String[]> getCompletedCommands() {
		return completedCommands;
	}

	public ArrayList<String> getProcessOutput() {
		return processOutput;
	} 

}


