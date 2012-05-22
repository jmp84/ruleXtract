/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;

/**
 * @author jmp84 Main program to retrieve a set specific rule file for a given
 *         test set.
 */
public class Retrieval extends Configured implements Tool {

    /**
     * @param args
     */
    public int run(String[] args) throws IOException {
        // read and parse the config
        String configFile = args[0];
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = getConf();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        // first step: get the source pattern instances
        PatternInstanceCreator2 patternInstanceCreator =
                new PatternInstanceCreator2(conf);
        String testFile = conf.get("testfile");
        if (testFile == null) {
            System.err.println("Missing property 'testfile' in the config");
            System.exit(1);
        }
        Set<Rule> sourcePatternInstances =
                patternInstanceCreator.createSourcePatternInstances(testFile);
        // second step: retrieve the rules
        RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(conf);
        List<GeneralPairWritable3> rules =
                ruleFileBuilder.getRules(sourcePatternInstances);
        // third step: build features
        List<GeneralPairWritable3> rulesWithFeatures =
                ruleFileBuilder.getRulesWithFeatures(conf, rules);
        ruleFileBuilder.writeSetSpecificRuleFile(
                rulesWithFeatures, conf.get("rules_out"));
        // TODO what to return
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
            System.exit(1);
        }
        int res = ToolRunner.run(new Retrieval(), args);
        System.exit(res);
    }
}
