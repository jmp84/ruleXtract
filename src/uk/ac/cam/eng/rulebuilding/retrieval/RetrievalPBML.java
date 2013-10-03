/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 Main program to retrieve a set specific rule file for a given
 *         test set. Special version for PBML that doesn't do filtering and that
 *         dumps retrieved rules directly to a file. Why: this way we can
 *         compare to Joshua retrieval (that doesn't do filtering) and also by
 *         dumping directly to a file, we avoid memory problems
 */
public class RetrievalPBML extends Configured implements Tool {

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
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        PatternInstanceCreator2 patternInstanceCreator =
                new PatternInstanceCreator2(conf);
        String testFile = conf.get("testfile");
        if (testFile == null) {
            System.err.println("Missing property 'testfile' in the config");
            System.exit(1);
        }
        Set<Rule> sourcePatternInstances =
                patternInstanceCreator.createSourcePatternInstances(testFile);
        stopWatch.stop();
        System.err.println("Pattern instance creation took "
                + stopWatch.getTime() + " milliseconds");
        // second step: retrieve the rules
        RuleFileBuilderPBML ruleFileBuilder = new RuleFileBuilderPBML(conf);
        ruleFileBuilder.getRulesPBML(sourcePatternInstances,
                conf.get("rules_out"));
        // TODO what to return
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
            System.exit(1);
        }
        int res = ToolRunner.run(new RetrievalPBML(), args);
        System.exit(res);
    }
}
