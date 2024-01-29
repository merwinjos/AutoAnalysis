package edu.utah.hci.auto;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Launcher {

	public static void main(String[] args) {
		if (args.length ==0){
			printDocs();
			System.exit(0);
		}
		else {
			boolean chpc = processArgs(args);
			if (chpc) new ChpcAutoAnalysis(args);
			else new GNomExAutoAnalysis (args);
		}

	}
	
	/**This method will process each argument and assign new variables*/
	public static boolean processArgs(String[] args){
		Pattern pat = Pattern.compile("-[a-z]");
		String location = null;
		for (int i = 0; i<args.length; i++){
			String lcArg = args[i].toLowerCase();
			Matcher mat = pat.matcher(lcArg);
			if (mat.matches()){
				char test = args[i].charAt(1);
				try{
					switch (test){
						case 'l': location = args[++i].toLowerCase(); break;
					}
				}
				catch (Exception e){
					Util.printErrAndExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
				}
			}
		}
		//any location info?
		if (location == null) {
			printDocs();
			System.exit(0);
		}
		if (location.contains("chpc")) return true;
		return false;
	}

	
	public static void printDocs(){
		Util.pl("\n" +
				"**************************************************************************************\n" +
				"**                              Auto Analysis: Jan 2024                             **\n" +
				"**************************************************************************************\n" +
				"AA orchestrates auto analysis of sequencing files in GNomEx Experiment using two\n"+
				"different daemons running on HCI or on CHPC. See xxxx for details.\n"+

				"\nOptions:\n"+
				"   -l Location running this jar file, either CHPC or HCI.\n"+
				"   -c Path to the auto analysis configuration file.\n"+
				"   -v Produce verbose debugging output.\n"+
				"   -d Dry run, just print ssh and rsync cmds.\n"+
				

				"\nExample: java -jar AutoAnalysis.xxx.jar -l CHPC -c autoAnalysis.config.txt -v \n\n"+


				"**************************************************************************************\n");
	}

}
