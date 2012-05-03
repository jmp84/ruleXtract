/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;
import uk.ac.cam.eng.rulebuilding.hadoop.RuleRetrievalJob;
import uk.ac.cam.eng.rulebuilding.hadoop.SourcePatternInstancesSortJob;

/**
 * @author jmp84 Main program to retrieve a set specific rule file for a given
 *         test set.
 */
public class Retrieval extends Configured implements Tool {

    /**
     * Creates source pattern instances from a test file and dumps them to a
     * SequenceFile in sorted order for further processing.
     * 
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     */
    public void sourcePatternInstances2SequenceFile(Configuration conf)
            throws FileNotFoundException, IOException {
        PatternInstanceCreator2 patternInstanceCreator =
                new PatternInstanceCreator2(conf);
        String testFile = conf.get("testfile");
        if (testFile == null) {
            System.err.println("Missing property 'testfile' in the config");
            System.exit(1);
        }
        String outFile = conf.get("work_dir") + "/source_pattern_instances";
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outFile);
        Set<Rule> sourcePatternInstances =
                patternInstanceCreator.createSourcePatternInstances(testFile);
        SequenceFile.Writer writer =
                new SequenceFile.Writer(fs, conf, path, RuleWritable.class,
                        NullWritable.class);
        for (Rule sourcePatternInstance: sourcePatternInstances) {
            RuleWritable sourcePatternInstanceWritable =
                    RuleWritable.makeSourceMarginal(sourcePatternInstance);
            writer.append(sourcePatternInstanceWritable, NullWritable.get());
        }
        writer.close();
    }

    public void buildFeatures(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path inputPathPattern =
                new Path(conf.get("work_dir") + "/rules_mr_features/part*");
        FileStatus[] inputs = fs.globStatus(inputPathPattern);
        List<GeneralPairWritable3> rules = new ArrayList<>();
        for (FileStatus input: inputs) {
            Path path = input.getPath();
            SequenceFile.Reader reader =
                    new SequenceFile.Reader(fs, path, conf);
            Writable key = new GeneralPairWritable3();
            while (reader.next(key)) {
                rules.add((GeneralPairWritable3) WritableUtils.clone(key, conf));
                // we need to clear the map containing the mapreduce features,
                // otherwise the map becomes dense instead of staying sparse
                ((GeneralPairWritable3) key).getSecond().clear();
            }
            reader.close();
        }
        RuleFileBuilder ruleFileBuilder = new RuleFileBuilder(conf);
        List<GeneralPairWritable3> rulesWithFeatures =
                ruleFileBuilder.getRulesWithFeatures(conf, rules);
        ruleFileBuilder.writeSetSpecificRuleFile(
                rulesWithFeatures, conf.get("rules_out"));
    }

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
        // set up the job flow
        JobControl jobControl = new JobControl("Retrieval");
        // first step: dump the source pattern instances to a sequence file
        sourcePatternInstances2SequenceFile(conf);
        // second step: sort the source pattern instances
        HadoopJob sourcePatternSortJob = new SourcePatternInstancesSortJob();
        ControlledJob controlledSourcePatternSortJob =
                new ControlledJob(sourcePatternSortJob.getJob(conf), null);
        jobControl.addJob(controlledSourcePatternSortJob);
        // third step: retrieve the rules
        HadoopJob ruleRetrievalJob = new RuleRetrievalJob();
        ControlledJob controlledRuleRetrievalJob =
                new ControlledJob(ruleRetrievalJob.getJob(conf), null);
        controlledRuleRetrievalJob
                .addDependingJob(controlledSourcePatternSortJob);
        jobControl.addJob(controlledRuleRetrievalJob);
        // run second and third step
        Thread control = new Thread(jobControl);
        control.start();
        while (!jobControl.allFinished()) {
            try {
                Thread.sleep(5000);
            }
            catch (Exception e) {}
        }
        // fourth step: build features
        buildFeatures(conf);
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
