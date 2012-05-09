/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;

/**
 * @author juan MapReduce job to sort source pattern instances according to
 *         HFile order
 */
public class SourcePatternInstancesSortJob implements HadoopJob {

    public Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "Source pattern instances sort");
        job.setJarByClass(SourcePatternInstancesSortJob.class);
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(NullWritable.class);
        // identity mapper
        job.setMapperClass(Mapper.class);
        // identity reducer
        job.setReducerClass(Reducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // sort sources in the same order as the keys of the HFile where we will
        // do a lookup.
        job.setSortComparatorClass(Bytes.ByteArrayComparator.class);
        // only one reduce task so that the input to the rule retrieval job
        // is not splitted too much
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, conf.get("work_dir")
                + "/source_pattern_instances");
        FileOutputFormat.setOutputPath(job, new Path(conf.get("work_dir")
                + "/source_pattern_instances_sorted"));
        FileOutputFormat.setCompressOutput(job, true);
        return job;
    }
}
