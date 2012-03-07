/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 MapReduce job to compute source-to-target probability
 */
public class Source2TargetProbabilityJob extends
        Mapper<RuleWritable, IntWritable, RuleWritable, FeatureWritable> {

}
