/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.mapreduce.Partitioner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RulePatternWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Partitions rules by their source pattern. Used to compute the
 *         source-to-target pattern translation probability.
 */
public class SourcePatternPartitioner<V> extends
        Partitioner<RuleWritable, V> {

    public int getPartition(RuleWritable key, V value, int numReduceTasks) {
        RulePatternWritable pattern = new RulePatternWritable(key);
        RulePatternWritable sourcePattern = pattern.makeSourceMarginal();
        return (sourcePattern.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
