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

/**
 * @author jmp84 MapReduce job to compute binary provenance
 */
public class BinaryProvenanceJob implements MapReduceFeature {

    private final static String name = "binary_provenance";

    public int getNumberOfFeatures(Configuration conf) {
        int res = conf.getInt(name, 0);
        if (res == 0) {
            System.err.println("ERROR: missing property 'binary_provenance' " +
                    "to indicate the number of features " +
                    "for this feature class");
            System.exit(1);
        }
        return res;
    }

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, name);
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
        FileInputFormat.setInputPaths(job, conf.get("work_dir") + "/rules");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir") + "/"
                + name));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Reducer to compute binary provenance feature. Simply merge the binary
     * features into a map and taking the offset into account.
     */
    private static class BinaryProvenanceReducer extends
            Reducer<RuleWritable, RuleInfoWritable, RuleWritable, MapWritable> {

        /**
         * Starting index for this mapreduce feature. This is given by a config
         * and set in the setup method.
         */
        private static int featureStartIndex;

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
            featureStartIndex = conf.getInt(name, 0);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(RuleWritable key,
                Iterable<RuleInfoWritable> values, Context context)
                throws IOException, InterruptedException {
            // need to clear, otherwise the provenances cumulate
            // not needed for other features where the same indices are reused
            // and the values are overwritten
            features.clear();
            for (RuleInfoWritable ruleInfoWritable: values) {
                for (Writable provenance: ruleInfoWritable.getProvenance()
                        .keySet()) {
                    // provenance can be either a string (for example to
                    // indicate a genre/collection provenance like nw, web,
                    // etc.) or an integer (for example to indicate a
                    // provenance like training instance id)
                    try {
                        int featureIndexInt =
                                Integer.parseInt(provenance.toString());
                        IntWritable featureIndex =
                                new IntWritable(featureStartIndex
                                        + featureIndexInt);
                        features.put(featureIndex, one);
                    }
                    catch (NumberFormatException e) {
                        // the provenance is not an integer, continue to the
                        // next provenance
                        continue;
                    }
                }
            }
            context.write(key, features);
        }
    }
}
