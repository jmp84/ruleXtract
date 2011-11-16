/**
 * 
 */

package uk.ac.cam.eng.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
        if (args.length != 2) {
            System.err.println("Args: <word map> <PTB tree file>");
            System.exit(1);
        }
        Map<String, String> wordMap = new HashMap<String, String>();
        try (BufferedReader br =
                new BufferedReader(new FileReader(args[0]))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                wordMap.put(parts[1], parts[0]);
            }
        }
        try (BufferedReader br =
                new BufferedReader(new FileReader(args[1]))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Tree parseTree = Tree.valueOf(line);
                Tree parseTree = Trees.PennTreeReader.parseEasy(line);
                Iterator<Tree> parseTreeIterator =
                        parseTree.iterator();
                @SuppressWarnings("unused")
                int a = 0;
                while (parseTreeIterator.hasNext()) {
                    Tree next = parseTreeIterator.next();
                    if (next.isLeaf()) {
                        String word = (String) next.getLabel();
                        // next.setValue(wordMap.get(word));
                        next.setLabel(wordMap.get(word));
                    }
                }
                System.out.println(parseTree.toString());
            }
        }
    }
}
