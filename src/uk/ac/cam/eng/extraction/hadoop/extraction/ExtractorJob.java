
package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.RuleExtractor;
import uk.ac.cam.eng.extraction.datatypes.Alignment;
import uk.ac.cam.eng.extraction.datatypes.Rule;
import uk.ac.cam.eng.extraction.datatypes.SentencePair;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleInfoWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.TextArrayWritable;

public class ExtractorJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "rules");
        job.setJarByClass(ExtractorJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(RuleInfoWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(RuleInfoWritable.class);
        job.setMapperClass(ExtractorMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // no reducer
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(
                job, conf.get("work_dir") + "/training_data");
        FileOutputFormat.setOutputPath(
                job, new Path(conf.get("work_dir") + "/rules"));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }

    /**
     * Mapper for rule extraction. Extracts the rules and writes the rule and
     * additional info (unaligned words, etc.). We separate the rule from its
     * additional info to be flexible and avoid equality problems whenever we
     * add more info to the rule. The output will be the input to mapreduce
     * features.
     */
    private static class ExtractorMapper
            extends
            Mapper<MapWritable, TextArrayWritable, RuleWritable, RuleInfoWritable> {

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void
                map(MapWritable key, TextArrayWritable value, Context context)
                        throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String sentenceAlign = ((Text) value.get()[0]).toString();
            String wordAlign = ((Text) value.get()[1]).toString();
            boolean side1source = conf.getBoolean("side1source", false);
            SentencePair sp = new SentencePair(sentenceAlign, side1source);
            Alignment a = new Alignment(wordAlign, sp, side1source);
            RuleExtractor re = new RuleExtractor(conf);
            for (Rule r: re.extract(a, sp)) {
                // TODO replace with static objects ?
                RuleWritable rw = new RuleWritable(r);
                RuleInfoWritable riw = new RuleInfoWritable(r);
                // the key is the set of provenances of the instance
                riw.setProvenance(key);
                context.write(rw, riw);
            }
        }
    }
}
