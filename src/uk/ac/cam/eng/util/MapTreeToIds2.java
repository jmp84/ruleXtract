/**
 * 
 */

package uk.ac.cam.eng.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.berkeley.nlp.syntax.Tree;
import edu.berkeley.nlp.syntax.Trees;

/**
 * @author jmp84 Simple utility to read a file containing parse trees in Penn
 *         Treebank (PTB) format and convert the leaves to word ids.
 */
public class MapTreeToIds2 {

    /**
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        if (args.length != 3) {
            System.err.println("Args: <word map> <PTB tree file> <words>");
            System.exit(1);
        }
        Map<String, String> wordMap = new HashMap<String, String>();
        Map<String, String> wordMapReverse = new HashMap<String, String>();
        try (BufferedReader br =
                // new BufferedReader(new FileReader(args[0]))) {
                new BufferedReader(new InputStreamReader(new FileInputStream(
                        args[0]), "UTF-16"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                System.out.println(line);
                wordMap.put(parts[1], parts[0]);
                if (parts[0].equals("108897") || parts[0].equals("939751")) {
                    System.out.println(line);
                }
                wordMapReverse.put(parts[0], parts[1]);
            }
        }
        System.err.println(wordMapReverse.get("939751").equals(
                wordMapReverse.get("108897")));
        System.out.println(wordMapReverse.get("939751"));
        System.out.println(wordMapReverse.get("108897"));
        System.out.println((int) wordMapReverse.get("108897").charAt(9));
        System.out.println((int) wordMapReverse.get("939751").charAt(9));
        System.out.println((int) wordMapReverse.get("939751").charAt(10));
        try (BufferedReader br =
                // new BufferedReader(new FileReader(args[1]));
                new BufferedReader(new InputStreamReader(new FileInputStream(
                        args[1]), "UTF-8"));
                BufferedReader brWords =
                        // new BufferedReader(new FileReader(args[2]))) {
                        new BufferedReader(new InputStreamReader(
                                new FileInputStream(args[2]), "UTF-8"));
                BufferedWriter bw =
                        new BufferedWriter(
                                new FileWriter(
                                        "/home/blue9/jmp84/exps/0065-ZHENc4v1p1/sub3/etrees/tempmirror"))) {
            String line;
            String lineWords;
            while ((line = br.readLine()) != null
                    && (lineWords = brWords.readLine()) != null) {
                if (line.equals("(())")) {
                    continue; // empty parse
                }
                String[] words = lineWords.split("\\s+");
                Tree parseTree = Trees.PennTreeReader.parseEasy(line);
                Iterator<Tree> parseTreeIterator =
                        parseTree.iterator();
                int index = 0;
                while (parseTreeIterator.hasNext()) {
                    Tree next = parseTreeIterator.next();
                    if (next.isLeaf()) {
                        String word = (String) next.getLabel();
                        String wordCheck = words[index];
                        bw.write(wordCheck + " ");
                        String id = wordMap.get(wordCheck);
                        if (word.equals(wordCheck)) {
                            next.setLabel(wordMap.get(word));
                        }
                        // this means that the word was discarded in parsing and
                        // only the tag was left. I an empty tag was left, we
                        // set the tag to be 'X'
                        else {
                            // next.setLabel(wordMap.get(wordCheck));
                            List<Tree> children = new ArrayList<Tree>();
                            children.add(new Tree(wordMap.get(wordCheck)));
                            next.setChildren(children);
                            if (((String) next.getLabel()).isEmpty()) {
                                next.setLabel("X");
                            }
                        }
                        index++;
                    }
                }
                System.out.println(parseTree.toString());
            }
        }
    }
}
