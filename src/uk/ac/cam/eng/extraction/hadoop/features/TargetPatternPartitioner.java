/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.mapreduce.Partitioner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 Partitions rules by their source pattern. Used to compute the
 *         source-to-target pattern translation probability.
 */
public class TargetPatternPartitioner<V> extends Partitioner<RuleWritable, V> {

    public int getPartition(RuleWritable key, V value, int numReduceTasks) {
        RuleWritable targetPattern = key.getTargetPattern();
        return (targetPattern.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
