/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author juan
 * This class is a utility to filter a set specific rule file
 * as created with the old pipeline with source pattern
 * instances that occur in the test set
 */
public class OldRulefileFilter {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		if (args.length != 2) {
			System.err.println("Args: <rule file builder config> <set specific rule file>");
		}
		RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(args[0]);
		Properties p = new Properties();
        try {
            p.load(new FileInputStream(args[0]));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String testFile = p.getProperty("testfile");
        String patternFile = p.getProperty("patternfile");
		Set<Rule> sourceRules = ruleFileBuilder.getSourceRuleInstances(patternFile, testFile);
		try (BufferedReader br = new BufferedReader(new FileReader(args[1]))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\\s+");
				Rule r = new Rule(parts[1], "");
				if (sourceRules.contains(r)) {
					System.out.println(line);
				}
			}
		}
	}
}
