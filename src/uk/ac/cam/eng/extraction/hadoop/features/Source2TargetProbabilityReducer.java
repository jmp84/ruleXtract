/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3ArrayWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.extraction.hadoop.extraction.ExtractorReducerMethod3.ValueComparator;

/**
 * @author jmp84 Reducer to compute target-to-source probability
 */
public class Source2TargetProbabilityReducer extends 
		Reducer<RuleWritable, PairWritable, RuleWritable, MapWritable> {

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
	sortMapByValue(Map<K, V> unsortedMap) {
		SortedMap<K, V> sortedMap = new TreeMap<K, V>(
				new ValueComparator<K, V>(unsortedMap));
		sortedMap.putAll(unsortedMap);
		return sortedMap;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(RuleWritable key, Iterable<PairWritable> values, Context context)
			throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // first loop through the targets and gather counts
        double marginalCount = 0;
        Map<RuleWritable, Integer> ruleCounts =
                new TreeMap<RuleWritable, Integer>();
        for (PairWritable targetAndCount: values) {
            marginalCount += targetAndCount.second.get();
            RuleWritable rw = new RuleWritable();
            if (source2target) {
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
                ruleCounts.put(rw, sideCountPair.second.get());
            }
            else {
                ruleCounts.put(rw,
                        ruleCounts.get(rw) + sideCountPair.second.get());
            }
            // compute unaligned word feature in the source2target
            if (!ruleAndUnalignedSourceWords.containsKey(rw)) {
                ruleAndUnalignedSourceWords.put(rw, sideCountPair.first
                        .getNumberUnalignedSourceWords().get());
            }
            else {
                ruleAndUnalignedSourceWords.put(rw,
                        ruleAndUnalignedSourceWords.get(rw)
                                + sideCountPair.first
                                        .getNumberUnalignedSourceWords()
                                        .get());
            }
            if (!ruleAndUnalignedTargetWords.containsKey(rw)) {
                ruleAndUnalignedTargetWords.put(rw, sideCountPair.first
                        .getNumberUnalignedTargetWords().get());
            }
            else {
                ruleAndUnalignedTargetWords.put(rw,
                        ruleAndUnalignedTargetWords.get(rw)
                                + sideCountPair.first
                                        .getNumberUnalignedTargetWords()
                                        .get());
            }
        }
        // do a second pass for normalization
        // first sort ruleCounts by value (sorting by count is the same as
        // sorting by probability because here the denominator is the same)
        // Map<RuleWritable, Integer> sortedMap = sortMapByValue(ruleCounts);
        PairWritable3[] outputValueArray = new PairWritable3[ruleCounts.size()];
        int i = 0;
        for (RuleWritable rw: ruleCounts.keySet()) {
            double countRule = ruleCounts.get(rw);
            DoubleWritable probability = new DoubleWritable(countRule
                    / marginalCount);
            // TODO either respect nbFeatures or just have default base features
            DoubleWritable[] features = new DoubleWritable[nbFeatures];
            features[0] = source2target ? probability : new DoubleWritable(0);
            features[1] = source2target ? new DoubleWritable(0) : probability;
            features[2] = new DoubleWritable(countRule);
            if (source2target && nbFeatures >= 5) { // unaligned word feature
                DoubleWritable averageUnalignedSource =
                        new DoubleWritable(ruleAndUnalignedSourceWords.get(rw)
                                / countRule);
                DoubleWritable averageUnalignedTarget =
                        new DoubleWritable(ruleAndUnalignedTargetWords.get(rw)
                                / countRule);
                features[3] = averageUnalignedSource;
                features[4] = averageUnalignedTarget;
            }
            ArrayWritable featuresWritable =
                    new ArrayWritable(DoubleWritable.class, features);
            outputValueArray[i] = new PairWritable3(rw, featuresWritable);
            i++;
        }
        PairWritable3ArrayWritable outputValue =
                new PairWritable3ArrayWritable();
        outputValue.set(outputValueArray);
        context.write(key, outputValue);
	}

	
	
}
