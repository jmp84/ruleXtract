/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;

/**
 * @author jmp84 MapReduce job to compute binary provenance
 */
public class BinaryProvenanceJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "binary_provenance");
        job.setJarByClass(BinaryProvenanceJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(RuleInfoWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(MapWritable.class);
        // identity mapper
        job.setMapperClass(Mapper.class);
        job.setReducerClass(BinaryProvenanceReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Reducer to compute binary provenance feature. Simply merge the binary
     * features into a map and taking the offset into account.
     */
    private static class BinaryProvenanceReducer
            extends
            Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;
        /**
         * Name of the feature class. This is hard coded and used to retrieve
         * featureStartIndex from a config.
         */
        private static String featureName = "binary_provenance";

        // static writables to avoid memory consumption
        private static MapWritable features = new MapWritable();
        private static IntWritable one = new IntWritable(1);

        /*
         * (non-Javadoc)
         * @see
         * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            // TODO add a check here
            featureStartIndex = conf.getInt(featureName, 0);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(RuleWritable key,
                Iterable<RuleInfoWritable> values,
                Context context) throws IOException, InterruptedException {
            // need to clear, otherwise the provenances cumulate
            // not needed for other features where the same indices are reused
            // and the values are overwritten
            features.clear();
            for (RuleInfoWritable ruleInfoWritable: values) {
                for (Writable provenance: ruleInfoWritable
                        .getBinaryProvenance()
                        .keySet()) {
                    IntWritable featureIndex =
                            new IntWritable(featureStartIndex
                                    + ((IntWritable) provenance).get());
                    features.put(featureIndex, one);
                }
            }
            context.write(key, features);
        }
    }
}
