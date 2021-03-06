/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;
import uk.ac.cam.eng.util.Pair;

/**
 * @author juan
 */
public class HFileCreatorMergeProvenance2 extends Configured {

    private static byte[] object2ByteArray(Writable obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        obj.write(out);
        return buffer.toByteArray();
    }

    private static ArrayWritable bytes2ArrayWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        ArrayWritable value = new ArrayWritable(PairWritable3.class);
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }

    private static RuleWritable bytes2RuleWritable(ByteBuffer bytes) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes.array(), bytes.arrayOffset(), bytes.limit());
        RuleWritable value = new RuleWritable();
        try {
            value.readFields(in);
        }
        catch (IOException e) {
            // Byte buffer is memory backed so no exception is possible. Just in
            // case chain it to a runtime exception
            throw new RuntimeException(e);
        }
        return value;
    }

    private static int getLength(int index) {
        return (index == 0) ? 5 : 5 + 3 * index;
    }

    private static int getOffset(int index) {
        return (index == 0) ? 0 : 5 + 3 * (index - 1);
    }

    // private static List<PairWritable3> merge(
    // List<Pair<ArrayWritable, Integer>> toMerge, int featureLength) {
    // List<PairWritable3> res = new ArrayList<>();
    // List<PairWritable3> toBeProcessed = new ArrayList<>();
    // // initialized toBeProcessed with the first element of each HFile
    // for (Pair<ArrayWritable, Integer> targetsAndFeatures: toMerge) {
    // toBeProcessed.add((PairWritable3) targetsAndFeatures.getFirst()
    // .get()[0]);
    // }
    // boolean finished = false;
    // boolean[] nonext = new boolean[toMerge.size()];
    // boolean[] endOfFile = new boolean[toMerge.size()];
    // int[] targetIndices = new int[toMerge.size()];
    // while (!finished) {
    // // process toBeProcessed first
    // // find the minimum element
    // RuleWritable minTarget = null;
    // for (int i = 0; i < toBeProcessed.size(); i++) {
    // if (minTarget == null && !endOfFile[i]) {
    // minTarget = toBeProcessed.get(i).first;
    // continue;
    // }
    // if (endOfFile[i]) {
    // continue;
    // }
    // int cmp = minTarget.compareYield(toBeProcessed.get(i).first);
    // if (cmp > 0) {
    // minTarget = toBeProcessed.get(i).first;
    // }
    // }
    // List<DoubleWritable> mergedFeaturesList = new ArrayList<>();
    // for (int i = 0; i < toBeProcessed.size(); i++) {
    // int currentFeatureSize = mergedFeaturesList.size();
    // if (endOfFile[i]) {
    // continue;
    // }
    // int cmp = minTarget.compareYield(toBeProcessed.get(i).first);
    // if (cmp == 0) {
    // nonext[i] = false;
    // for (int j = currentFeatureSize; j < getOffset(toMerge.get(
    // i).getSecond()); j++) {
    // mergedFeaturesList.add(new DoubleWritable(0));
    // }
    // for (int j = 0; j < toBeProcessed.get(i).second.get().length; j++) {
    // mergedFeaturesList
    // .add((DoubleWritable) toBeProcessed.get(i).second
    // .get()[j]);
    // }
    // currentFeatureSize = mergedFeaturesList.size();
    // for (int j = currentFeatureSize; j < getLength(toMerge.get(
    // i).getSecond()); j++) {
    // mergedFeaturesList.add(new DoubleWritable(0));
    // }
    // }
    // else if (cmp < 0) {
    // nonext[i] = true;
    // }
    // else {
    // System.err.println("ERROR: " + minTarget
    // + " is supposed to be smaller than "
    // + toBeProcessed.get(i).first);
    // System.exit(1);
    // }
    // }
    // for (int j = mergedFeaturesList.size(); j < featureLength; j++) {
    // mergedFeaturesList.add(new DoubleWritable(0));
    // }
    // DoubleWritable[] mergedFeaturesArray =
    // mergedFeaturesList.toArray(new DoubleWritable[]{});
    // ArrayWritable mergedFeatures =
    // new ArrayWritable(DoubleWritable.class);
    // mergedFeatures.set(mergedFeaturesArray);
    // res.add(new PairWritable3(minTarget, mergedFeatures));
    // finished = true;
    // // refill toBeProcessed as needed
    // for (int i = 0; i < toMerge.size(); i++) {
    // if (!nonext[i]) {
    // if (!endOfFile[i]
    // && ++targetIndices[i] < toMerge.get(i).getFirst()
    // .get().length) {
    // finished = false;
    // toBeProcessed.set(i, (PairWritable3) toMerge.get(i)
    // .getFirst().get()[targetIndices[i]]);
    // }
    // else {
    // endOfFile[i] = true;
    // }
    // }
    // else {
    // // at least one element remains to be processed
    // finished = false;
    // }
    // }
    // }
    // return res;
    // }

    private static List<Pair<RuleWritable, List<Double>>> merge(
            List<Pair<ArrayWritable, Integer>> toMerge, int featureLength) {
        List<Pair<RuleWritable, List<Double>>> res = new ArrayList<>();
        List<PairWritable3> toBeProcessed = new ArrayList<>();
        // initialized toBeProcessed with the first element of each HFile
        for (Pair<ArrayWritable, Integer> targetsAndFeatures: toMerge) {
            toBeProcessed.add((PairWritable3) targetsAndFeatures.getFirst()
                    .get()[0]);
        }
        boolean finished = false;
        boolean[] nonext = new boolean[toMerge.size()];
        boolean[] endOfFile = new boolean[toMerge.size()];
        int[] targetIndices = new int[toMerge.size()];
        while (!finished) {
            // process toBeProcessed first
            // find the minimum element
            RuleWritable minTarget = null;
            for (int i = 0; i < toBeProcessed.size(); i++) {
                if (minTarget == null && !endOfFile[i]) {
                    minTarget = toBeProcessed.get(i).first;
                    continue;
                }
                if (endOfFile[i]) {
                    continue;
                }
                int cmp = minTarget.compareYield(toBeProcessed.get(i).first);
                if (cmp > 0) {
                    minTarget = toBeProcessed.get(i).first;
                }
            }
            List<Double> mergedFeaturesList = new ArrayList<>();
            for (int i = 0; i < toBeProcessed.size(); i++) {
                int currentFeatureSize = mergedFeaturesList.size();
                if (endOfFile[i]) {
                    continue;
                }
                int cmp = minTarget.compareYield(toBeProcessed.get(i).first);
                if (cmp == 0) {
                    nonext[i] = false;
                    for (int j = currentFeatureSize; j < getOffset(toMerge.get(
                            i).getSecond()); j++) {
                        mergedFeaturesList.add(0.0);
                    }
                    for (int j = 0; j < toBeProcessed.get(i).second.get().length; j++) {
                        mergedFeaturesList
                                .add(((DoubleWritable) toBeProcessed.get(i).second
                                        .get()[j]).get());
                    }
                    currentFeatureSize = mergedFeaturesList.size();
                    for (int j = currentFeatureSize; j < getLength(toMerge.get(
                            i).getSecond()); j++) {
                        mergedFeaturesList.add(0.0);
                    }
                }
                else if (cmp < 0) {
                    nonext[i] = true;
                }
                else {
                    System.err.println("ERROR: " + minTarget
                            + " is supposed to be smaller than "
                            + toBeProcessed.get(i).first);
                    System.exit(1);
                }
            }
            for (int j = mergedFeaturesList.size(); j < featureLength; j++) {
                mergedFeaturesList.add(0.0);
            }
            res.add(new Pair<RuleWritable, List<Double>>(minTarget,
                    mergedFeaturesList));
            finished = true;
            // refill toBeProcessed as needed
            for (int i = 0; i < toMerge.size(); i++) {
                if (!nonext[i]) {
                    if (!endOfFile[i]
                            && ++targetIndices[i] < toMerge.get(i).getFirst()
                                    .get().length) {
                        finished = false;
                        toBeProcessed.set(i, (PairWritable3) toMerge.get(i)
                                .getFirst().get()[targetIndices[i]]);
                    }
                    else {
                        endOfFile[i] = true;
                    }
                }
                else {
                    // at least one element remains to be processed
                    finished = false;
                }
            }
        }
        return res;
    }

    private static void mergeHFiles(List<String> inputHFiles,
            String outputHFile, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputHFile);
        if (fs.exists(path)) {
            System.out.println("Error: " + outputHFile + " already exists");
            System.exit(1);
        }
        // HFile.Writer writer = new HFile.Writer(fs, path, 20000000, "gz",
        // null);
        HFile.Writer writer = new HFile.Writer(fs, path, 64 * 1024, "gz", null);
        List<HFileScanner> scanners = new ArrayList<>();
        for (String inputHFile: inputHFiles) {
            HFile.Reader hfileReader =
                    new HFile.Reader(fs, new Path(inputHFile), null, false);
            hfileReader.loadFileInfo();
            HFileScanner hfileScanner = hfileReader.getScanner();
            scanners.add(hfileScanner);
        }
        List<Pair<ByteBuffer, ArrayWritable>> toBeProcessed =
                new ArrayList<>();
        // initialized toBeProcessed with the first element of each HFile
        for (int i = 0; i < scanners.size(); i++) {
            HFileScanner scanner = scanners.get(i);
            // go to the beginning of the file
            scanner.seekTo();
            // we assume here that the hfile is not empty
            // TODO find a way to check that
            ByteBuffer key = scanner.getKey();
            ArrayWritable value = bytes2ArrayWritable(scanner.getValue());
            toBeProcessed.add(new Pair<ByteBuffer, ArrayWritable>(key,
                    value));
        }
        boolean finished = false;
        // determine if we should scan the next element. initialize to false.
        // the first element will never be true
        boolean[] nonext = new boolean[scanners.size()];
        boolean[] endOfFile = new boolean[scanners.size()];
        while (!finished) {
            // process toBeProcessed first
            // find the minimum element
            ByteBuffer minSource = null;
            for (int i = 0; i < toBeProcessed.size(); i++) {
                if (minSource == null && !endOfFile[i]) {
                    minSource = toBeProcessed.get(i).getFirst();
                    continue;
                }
                if (endOfFile[i]) {
                    continue;
                }
                int cmp =
                        minSource.compareTo(toBeProcessed.get(i).getFirst());
                if (cmp > 0) {
                    minSource = toBeProcessed.get(i).getFirst();
                }
            }
            List<Pair<ArrayWritable, Integer>> toMerge = new ArrayList<>();
            for (int i = 0; i < toBeProcessed.size(); i++) {
                if (endOfFile[i]) {
                    continue;
                }
                int cmp =
                        minSource.compareTo(toBeProcessed.get(i).getFirst());
                if (cmp == 0) {
                    // minSources.add(toBeProcessed.get(i).getFirst().array());
                    nonext[i] = false;
                    // mergedList =
                    // merge(mergedList, toBeProcessed.get(i).getSecond(),
                    // i);
                    toMerge.add(new Pair<ArrayWritable, Integer>(toBeProcessed
                            .get(i).getSecond(), i));
                }
                else if (cmp < 0) {
                    nonext[i] = true;
                }
                else {
                    System.err.println("ERROR: "
                            + bytes2RuleWritable(minSource)
                            + " is supposed to be smaller than "
                            + bytes2RuleWritable(toBeProcessed.get(i)
                                    .getFirst()));
                    System.exit(1);
                }
            }
            int length = 5 + (toBeProcessed.size() - 1) * 3;
            // List<PairWritable3> mergedList = merge(toMerge, length);
            List<Pair<RuleWritable, List<Double>>> mergedList =
                    merge(toMerge, length);
            // PairWritable3[] mergedArray =
            // mergedList.toArray(new PairWritable3[]{});
            PairWritable3[] mergedArray = new PairWritable3[mergedList.size()];
            for (int i = 0; i < mergedArray.length; i++) {
                RuleWritable target = mergedList.get(i).getFirst();
                DoubleWritable[] featuresArray =
                        new DoubleWritable[mergedList.get(i).getSecond().size()];
                for (int j = 0; j < featuresArray.length; j++) {
                    featuresArray[j] =
                            new DoubleWritable(mergedList.get(i).getSecond()
                                    .get(j));
                }
                ArrayWritable features =
                        new ArrayWritable(DoubleWritable.class);
                features.set(featuresArray);
                mergedArray[i] = new PairWritable3(target, features);
            }
            ArrayWritable merged = new ArrayWritable(PairWritable3.class);
            merged.set(mergedArray);
            byte[] valueBytes = object2ByteArray(merged);
            // writer.append(minSource.array(), minSource.arrayOffset(),
            // minSource.array().length - minSource.arrayOffset(),
            // valueBytes, 0,
            // valueBytes.length);
            writer.append(minSource.array(), minSource.arrayOffset(),
                    minSource.limit(), valueBytes, 0,
                    valueBytes.length);
            finished = true;
            // refill toBeProcessed as needed
            for (int i = 0; i < scanners.size(); i++) {
                HFileScanner scanner = scanners.get(i);
                // get the next element
                if (!nonext[i]) {
                    if (!endOfFile[i] && scanner.next()) {
                        finished = false;
                        ByteBuffer key = scanner.getKey();
                        ArrayWritable value =
                                bytes2ArrayWritable(scanner.getValue());
                        toBeProcessed
                                .set(i, new Pair<ByteBuffer, ArrayWritable>(
                                        key, value));
                    }
                    else {
                        endOfFile[i] = true;
                    }
                }
                else {
                    // at least one element remains to be processed
                    finished = false;
                }
            }
        }
        writer.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out
                    .println("Usage: HFileCreatorMergeProvenance <config file>");
            System.exit(1);
        }
        String configFile = args[0];
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        String inputHFiles = p.getProperty("input_hfiles");
        if (inputHFiles == null) {
            System.err.println("ERROR: missing property input_hfiles");
            System.exit(1);
        }
        List<String> inputHFilesList = Arrays.asList(inputHFiles.split(","));
        String outputHFile = p.getProperty("output_hfile");
        if (outputHFile == null) {
            System.err.println("ERROR: missing property output_hfile");
            System.exit(1);
        }
        mergeHFiles(inputHFilesList, outputHFile, conf);
    }
}
