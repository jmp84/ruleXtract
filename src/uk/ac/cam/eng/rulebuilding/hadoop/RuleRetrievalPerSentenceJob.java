/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;
import uk.ac.cam.eng.rulebuilding.retrieval.PatternInstanceCreator2;
import uk.ac.cam.eng.rulebuilding.retrieval.RuleFileBuilder;
import uk.ac.cam.eng.rulebuilding.retrieval.SidePattern;

/**
 * @author jmp84 This class implements rule retrieval sentence by sentence. The
 *         hadoop framework here is only used for parallelization.
 * 
 */
public class RuleRetrievalPerSentenceJob implements HadoopJob {

    /*
     * (non-Javadoc)
     * 
     * @see
     * uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob#getJob(org.apache
     * .hadoop.conf.Configuration)
     */
    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "Rule Retrieval Per Sentence");
        job.setJarByClass(RuleRetrievalPerSentenceJob.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(RuleRetrievalPerSentenceMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // no reducer
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, "file://" + conf.get("testfile"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir")
                + "/rules_by_sentence"));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    private static class RuleRetrievalPerSentenceMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        private PatternInstanceCreator2 patternInstanceCreator;
        List<SidePattern> sidePatterns;
        private RuleFileBuilder ruleFileBuilder;

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            patternInstanceCreator = new PatternInstanceCreator2(conf);
            sidePatterns = patternInstanceCreator.createSourcePatterns();
            ruleFileBuilder = new RuleFileBuilder(conf);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Set<Rule> sourcePatternInstances =
                    patternInstanceCreator.createSourcePatternInstancesOneLine(
                            value.toString(), sidePatterns);
            List<GeneralPairWritable3> rules =
                    ruleFileBuilder.getRules(sourcePatternInstances);
            List<GeneralPairWritable3> rulesWithFeatures =
                    ruleFileBuilder.getRulesWithFeatures(conf, rules);
            ruleFileBuilder.writeSetSpecificRuleFile(rulesWithFeatures,
                    conf.get("rules_out"));
        }
    }
}
