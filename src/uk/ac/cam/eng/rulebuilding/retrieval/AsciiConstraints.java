/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Matchers;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class computes a list of ascii constraint rules.
 */
public class AsciiConstraints {

    private static int MAX_ASCII_CONSTRAINT_LENGTH = 5;

    public AsciiConstraints() {}

    public AsciiConstraints(Configuration conf) {
        MAX_ASCII_CONSTRAINT_LENGTH = conf.getInt(
                "max_ascii_constraint_length", 5);
    }

    public Set<Rule> getAsciiConstraints(String filename) throws IOException {
        Set<Rule> res = new HashSet<Rule>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            Pattern regex = Pattern.compile(".*: (.*) # (.*)");
            Matcher matcher;
            while ((line = br.readLine()) != null) {
                matcher = regex.matcher(line);
                if (matcher.matches()) {
                    String[] sourceString = matcher.group(1).split(" ");
                    String[] targetString = matcher.group(2).split(" ");
                    List<Integer> source = new ArrayList<Integer>();
                    List<Integer> target = new ArrayList<Integer>();
                    for (String ss: sourceString) {
                        source.add(Integer.parseInt(ss));
                    }
                    for (String ts: targetString) {
                        target.add(Integer.parseInt(ts));
                    }
                    Rule rule = new Rule(source, target);
                    res.add(rule);
                }
                else {
                    System.err.println("Malformed ascii constraint file: "
                            + filename);
                    System.exit(1);
                }
            }
        }
        return res;
    }
}
