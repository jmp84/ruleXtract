/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;
import uk.ac.cam.eng.rulebuilding.retrieval.RuleFileBuilder;

/**
 * TODO get the author right
 * 
 * @author juan MapReduce job to run rule retrieval. There is no reducer.
 */
public class RuleRetrievalJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "Rule Retrieval");
        job.setJarByClass(RuleRetrievalJob.class);
        job.setMapOutputKeyClass(GeneralPairWritable3.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(GeneralPairWritable3.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(RuleBuildingMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // no reducer
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, conf.get("work_dir")
                + "/source_pattern_instances_sorted");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir")
                + "/rules_mr_features"));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    private static class RuleBuildingMapper
            extends
            Mapper<RuleWritable, NullWritable, GeneralPairWritable3, NullWritable> {

        private RuleFileBuilder ruleFileBuilder;

        /*
         * (non-Javadoc)
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
         * Mapper.Context)
         */
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            ruleFileBuilder = new RuleFileBuilder(conf);
        }

        /**                                                                                                                                                                                             
         *                                                                                                                                  
         */
        @Override
        protected void
                map(RuleWritable key, NullWritable value, Context context)
                        throws java.io.IOException, InterruptedException {
            List<GeneralPairWritable3> rules = ruleFileBuilder.getRules(key);
            for (GeneralPairWritable3 rule: rules) {
                context.write(rule, NullWritable.get());
            }
        }
    }
}
