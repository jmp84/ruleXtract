/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3ArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.ExtractorJob;
import uk.ac.cam.eng.extraction.hadoop.extraction.ExtractorMapperMethod3;
import uk.ac.cam.eng.extraction.hadoop.extraction.ExtractorReducerMethod3;

/**
 * @author juan MapReduce job to run rule retrieval
 *
 */
public class RuleBuildingJob extends Configured implements Tool {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
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
        Job job = Job.getInstance(new Cluster(conf), conf);
        job.setJarByClass(ExtractorJob.class);
        job.setJobName("Rule Retrieval");
        job.setMapOutputKeyClass(?????.class);
        job.setMapOutputValueClass(?????.class);
        job.setOutputKeyClass(?????.class);
        job.setOutputValueClass(?????.class);
        job.setMapperClass(RuleBuildingMapper.class);
        job.setReducerClass(RuleBuildingReducer.class);
        job.setInputFormatClass(???.class);
        job.setOutputFormatClass(???.class);
        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));
        FileOutputFormat.setCompressOutput(job, true);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
        }
        int res = ToolRunner.run(new ExtractorJob(), args);
        System.exit(res);
	}
}
