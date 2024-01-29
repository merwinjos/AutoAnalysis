package edu.utah.hci.auto;

import java.util.regex.Matcher;

public class MiscDelme {

	public static void main(String[] args) {
		//parse the ID
		Matcher mat = Util.SLURM_JOBID.matcher("slurm-6491472.out");
		if (mat.matches()) {
			String id = mat.group(1);
			Util.pl(id);
		}
		

	}

}
