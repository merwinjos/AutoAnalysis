package edu.utah.hci.auto;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class GNomExRequest {
	
	private String[] dbResults;
	private String originalRequestId;
	private String requestIdCleaned;
	private String creationDate;
	private String requestorEmail;
	private String labGroupLastName;
	private String labGroupFirstName;
	private String organism;
	private String genomeBuild;
	private String libraryPreparation;
	private String analysisNotes;
	
	private File requestDirectory = null;
	private File[] fastqFiles = null;
	private File autoAnalysisMainDirectory = null;
	private File autoAnalysisJobsDirectory = null;
	
	/*Comma delimited, no spaces, full path, on Redwood, if a dir the contents will be copied into each job.*/
	private String workflowPaths = null;
	
	private String errorMessages = null;

	public GNomExRequest (String[] fields) {
		dbResults = fields;
		//watch out for requests with numbers trailing the R, e.g. 22564R1 -> converted to just 22564R which is the dir name in the repo
		int index = fields[0].lastIndexOf("R")+1;
		requestIdCleaned = fields[0].substring(0, index);
		originalRequestId = fields[0];
		
		creationDate = fields[1];
		requestorEmail = fields[2];
		labGroupLastName = fields[3];
		labGroupFirstName = fields[4];
		organism = fields[5].trim();
		genomeBuild = fields[6];
		libraryPreparation = fields[7].trim();
		analysisNotes = fields[8];
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder(dbResults[0]);
		for (int i=1; i< dbResults.length; i++) {
			sb.append("\n");
			sb.append(dbResults[i]);
		}
		return sb.toString();
	}
	
	public String simpleToString() {
		StringBuilder sb = new StringBuilder();
		sb.append(originalRequestId); sb.append("\t");
		sb.append(creationDate); sb.append("\t");
		sb.append(organism); sb.append("\t");
		sb.append(genomeBuild); sb.append("\t");
		sb.append(libraryPreparation);
		return sb.toString();
	}
	
	public boolean createAutoAnalysisJobs(File chpcLinkDirectory) {
		try {
			//create the dir AutoAnalysis_22Dec2023
			autoAnalysisMainDirectory = new File (requestDirectory, "AutoAnalysis_"+Util.getDateNoSpaces());
			autoAnalysisMainDirectory.mkdirs();
			autoAnalysisJobsDirectory = new File(autoAnalysisMainDirectory, "Jobs");
			autoAnalysisJobsDirectory.mkdir();
			if (autoAnalysisJobsDirectory.exists()==false) throw new IOException("ERROR: failed to create a job directory -> "+autoAnalysisJobsDirectory);

			//pull sample names
			HashSet<String> sampleIds = new HashSet<String>();
			for (File f: fastqFiles) {
				String[] split = Util.UNDERSCORE.split(f.getName());
				sampleIds.add(split[0]);
			}

			//create sub directories, copy in the workflow docs, link in the fastq lines
			ArrayList<File> toLink = new ArrayList<File>();
			for (String sampleId: sampleIds) {

				// make the sub dir
				File subDir = new File (autoAnalysisJobsDirectory, sampleId);
				subDir.mkdir();

				//link in the fastqs
				toLink.clear();
				for (File f: fastqFiles) if (f.getName().startsWith(sampleId+ "_")) toLink.add(f);
				Util.createSymbolicLinks(toLink, subDir);
				
				//link the sub dir to the chpcLinkDirectory
				toLink.clear();
				toLink.add(subDir);
				Util.createSymbolicLinks(toLink, chpcLinkDirectory);

				//add a RUNME.txt file
				String runMe = 
						"workflowPaths\t"+workflowPaths+
						"\norganism\t"+ organism+
						"\ngenomeBuild\t"+genomeBuild+
						"\nlibraryPrep\t"+libraryPreparation;
				Util.writeString(runMe, new File(subDir, "RUNME"));
			} 
			return true;
		} catch (Exception e) {
			Util.el("ERROR: making AutoAnalysis job for "+requestIdCleaned);
			e.printStackTrace();
			Util.deleteDirectory(autoAnalysisMainDirectory);
		}
		return false;
	}
	
	public boolean checkForAutoAnalysis() {
		File[] dirs = Util.extractFilesPrefix(requestDirectory, "AutoAnalysis_");
		if (dirs.length == 0) return false;
		else if (dirs.length == 1) autoAnalysisMainDirectory = dirs[0];
		else {
			//find most recent
			File mostRecent = dirs[0];
			long mostRecentTime = mostRecent.lastModified();
			for (int i=1; i< dirs.length; i++) {
				if (dirs[i].lastModified() > mostRecentTime) {
					mostRecent = dirs[i];
					mostRecentTime = mostRecent.lastModified();
				}
			}
			autoAnalysisMainDirectory = mostRecent;
		}
		//any Jobs directory?
		File jd = new File(autoAnalysisMainDirectory, "Jobs");
		if (jd.exists()) autoAnalysisJobsDirectory = jd;
		return true;
	}
	
	public boolean checkFastq() {
		File fastqDirectory = new File(requestDirectory, "Fastq");
		if (fastqDirectory.exists() == false) return false;
		//contains a file with md5 in the name
		File[] allFiles = Util.extractFiles(fastqDirectory);
		boolean foundMd5 = false;
		for (File f: allFiles) {		
			if (f.getName().contains("md5")) {
				foundMd5 = true;
				break;
			}
		}
		if (foundMd5 == false) return false;
		//find the fastq files and check they are all at least 1 hour old, want to avoid jobs in transfer from demuxing
		fastqFiles = Util.fetchFilesRecursively(fastqDirectory, "q.gz");
		long currentTime = System.currentTimeMillis() - 3600000;
		for (File f: fastqFiles) if ((currentTime - f.lastModified())<0) return false;
		return true;
	}

	public String getRequestIdCleaned() {
		return requestIdCleaned;
	}

	public String getCreationDate() {
		return creationDate;
	}

	public String getRequestorEmail() {
		return requestorEmail;
	}

	public String getLabGroupLastName() {
		return labGroupLastName;
	}

	public String getLabGroupFirstName() {
		return labGroupFirstName;
	}

	public String getOrganism() {
		return organism;
	}

	public String getGenomeBuild() {
		return genomeBuild;
	}

	public String getLibraryPreparation() {
		return libraryPreparation;
	}

	public String getAnalysisNotes() {
		return analysisNotes;
	}

	public File getRequestDirectory() {
		return requestDirectory;
	}

	public void setRequestDirectory(File requestDirectory) {
		this.requestDirectory = requestDirectory;
	}

	public String getErrorMessages() {
		return errorMessages;
	}

	public void setErrorMessages(String errorMessages) {
		this.errorMessages = errorMessages;
	}

	public String getOriginalRequestId() {
		return originalRequestId;
	}

	public File getAutoAnalysisMainDirectory() {
		return autoAnalysisMainDirectory;
	}

	public void setAutoAnalysisMainDirectory(File autoAnalysisMainDirectory) {
		this.autoAnalysisMainDirectory = autoAnalysisMainDirectory;
	}

	public File getAutoAnalysisJobsDirectory() {
		return autoAnalysisJobsDirectory;
	}

	public void setAutoAnalysisJobsDirectory(File autoAnalysisJobsDirectory) {
		this.autoAnalysisJobsDirectory = autoAnalysisJobsDirectory;
	}

	public String getWorkflowPaths() {
		return workflowPaths;
	}

	public void setWorkflowPaths(String workflowPaths) {
		this.workflowPaths = workflowPaths;
	}




}
