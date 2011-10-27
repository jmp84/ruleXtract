/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class implements the reducer for rule extraction described
 *         in "Fast, easy, cheap ..." by Chris Dyer et al. The output key is a
 *         RuleWritable that represents a source (resp. target). The output
 *         value is an ArrayWritable that represents a list of targets (resp.
 *         source) along with probabilities and other features (for example
 *         occurrence information). Each cell of the ArrayWritable will be a
 *         pair whose first element is a RuleWritable and second element is an
 *         ArrayWritable. We need to keep track of the features by array index.
 *         We use an array rather than an associative array for efficiency.
 *         Index 0 corresponds to the source-to-target probability, index 1
 *         corresponds to the target-to-source probability, index 2 corresponds
 *         to the rule occurrence count.
 */
public class ExtractorReducerMethod3
        extends
        // Reducer<RuleWritable, PairWritable, BytesWritable, ArrayWritable> {
        Reducer<BytesWritable, PairWritable, BytesWritable, ArrayWritable> {

    private static class ValueComparator<K extends Comparable<K>, V extends Comparable<V>>
            implements Comparator<K> {

        private final Map<K, V> map;

        public ValueComparator(Map<K, V> map) {
            super();
            this.map = map;
        }

        public int compare(K key1, K key2) {
            V value1 = this.map.get(key1);
            V value2 = this.map.get(key2);
            int c = value2.compareTo(value1);
            if (c != 0) {
                return c;
            }
            // compare the keys because in case of ties, we want to keep the
            // most frequent source rules (smaller integers correspond to more
            // frequent words)
            return key1.compareTo(key2);
        }
    }

    private static <K extends Comparable<K>, V extends Comparable<V>> Map<K, V>
            sortMapByValue(
                    Map<K, V> unsortedMap) {
        SortedMap<K, V> sortedMap = new TreeMap<K, V>(
                new ValueComparator<K, V>(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

    private byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    protected void reduce(BytesWritable key, Iterable<PairWritable> values,
            Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // default is 3: source-to-target, target-to-source, count
        // TODO check config
        int nbFeatures = conf.getInt("nb_features", 3);
        // default value is true: source-to-target extraction
        boolean source2target = conf.getBoolean("source2target", true);
        double marginalCount = 0;
        Map<RuleWritable, Integer> ruleCounts =
                new HashMap<RuleWritable, Integer>();
        // sideCountPair is either a target and a count (source-to-target
        // extraction) or a source and a count (target-to-source extraction)
        for (PairWritable sideCountPair: values) {
            marginalCount += sideCountPair.second.get();
            RuleWritable rw = new RuleWritable();
            if (source2target) {
                // rw.setSource(new Text(key.getSource()));
                rw.setSource(new Text());
                rw.setTarget(new Text(sideCountPair.first.getTarget()));
                rw.setLeftHandSide(new Text("0"));
            }
            else {
                rw.setSource(new Text(sideCountPair.first.getSource()));
                rw.setTarget(new Text());
                rw.setLeftHandSide(new Text("0"));
            }
            if (!ruleCounts.containsKey(rw)) {
                // ruleCounts.put(rw, targetCountPair.second);
                // ruleCounts.put(rw, count);
                ruleCounts.put(rw, sideCountPair.second.get());
            }
            else {
                // ruleCounts.put(rw, new IntWritable(ruleCounts.get(rw).get()
                // + targetCountPair.second.get()));
                // ruleCounts.put(rw, new IntWritable(ruleCounts.get(rw).get()
                // + count.get()));
                ruleCounts.put(rw,
                        ruleCounts.get(rw) + sideCountPair.second.get());
            }
        }
        // do a second pass for normalization
        // first sort ruleCounts by value (sorting by count is the same as
        // sorting by probability because here the denominator is the same)
        Map<RuleWritable, Integer> sortedMap = sortMapByValue(ruleCounts);
        PairWritable3[] outputValue = new PairWritable3[sortedMap.size()];
        // for (RuleWritable rw: ruleCounts.keySet()) {
        int i = 0;
        for (RuleWritable rw: sortedMap.keySet()) {
            // double countRule = ruleCounts.get(rw).get();
            double countRule = ruleCounts.get(rw);
            DoubleWritable probability = new DoubleWritable(countRule
                    / marginalCount);
            DoubleWritable[] features = new DoubleWritable[nbFeatures];
            features[0] = source2target ? probability : new DoubleWritable(0);
            features[1] = source2target ? new DoubleWritable(0) : probability;
            features[2] = new DoubleWritable(countRule);
            ArrayWritable featuresWritable =
                    new ArrayWritable(DoubleWritable.class, features);
            outputValue[i] = new PairWritable3(rw, featuresWritable);
            i++;
        }
        // context.write(rw, probability);
        context.write(key, new ArrayWritable(PairWritable3.class,
                outputValue));
    }
}
