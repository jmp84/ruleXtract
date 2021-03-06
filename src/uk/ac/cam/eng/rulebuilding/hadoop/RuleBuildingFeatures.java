/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.rulebuilding.retrieval.RuleFileBuilder;

/**
 * @author jmp84. Adds features and special rules to the rules retrieved.
 */
public class RuleBuildingFeatures {

    public static void main(String[] args) throws IOException,
            InterruptedException, ExecutionException {
        if (args.length != 1) {
            System.err.println("Args: config file");
        }
        String configFile = args[0];
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        FileSystem fs = FileSystem.get(conf);
        Path inputPathPattern = new Path(conf.get("outputPath") + "/part*");
        FileStatus[] inputs = fs.globStatus(inputPathPattern);
        List<GeneralPairWritable3> rules = new ArrayList<>();
        for (FileStatus input: inputs) {
            Path path = input.getPath();
            SequenceFile.Reader reader =
                    new SequenceFile.Reader(fs, path, conf);
            Writable key = new IntWritable();
            Writable value = new GeneralPairWritable3();
            while (reader.next(key, value)) {
                rules.add((GeneralPairWritable3) WritableUtils.clone(value,
                        conf));
            }
            reader.close();
        }
        RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(conf);
        List<GeneralPairWritable3> rulesWithFeatures =
                ruleFileBuilder.getRulesWithFeatures(conf, rules);
        ruleFileBuilder.writeSetSpecificRuleFile(
                rulesWithFeatures, conf.get("outputPathFeatures"));
    }
}
