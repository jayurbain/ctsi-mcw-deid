/**
 * @author jayurbain
 * 
 *  @version 12/28/2013, 9/2/2014
 */
package deidentification.mcw;

import java.io.*;
import java.util.*;

import deidentification.mcw.DeidentificationRegexMCW;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.*;

/**
 * MCW implementation of NamedEntityRecognition
 */
public class NamedEntityRecognitionMCW extends deidentification.NamedEntityRecognition {

	private StanfordCoreNLP pipeline = null;

	public NamedEntityRecognitionMCW() {

		Properties props = new Properties();
		// props.put("annotators",
		// "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
		pipeline = new StanfordCoreNLP(props);
	}

	public StanfordCoreNLP getPipeline() {
		return pipeline;
	}

	public void setPipeline(StanfordCoreNLP pipeline) {
		this.pipeline = pipeline;
	}

	public String performAnnotation(String text) {

		List<String> sentenceList = new ArrayList<String>();
		Annotation annotation = new Annotation(text);
		pipeline.annotate(annotation);
		// pipeline.prettyPrint(annotation, out);
		List<CoreMap> sentences = annotation
				.get(CoreAnnotations.SentencesAnnotation.class);

		if (sentences != null && sentences.size() > 0) {

			int lastEndPosition = 0;
			for (CoreMap sentence : sentences) {
				// traversing the words in the current sentence
				// a CoreLabel is a CoreMap with additional token-specific
				// methods
				StringBuffer sb = new StringBuffer();
				for (CoreLabel token : sentence.get(TokensAnnotation.class)) {

					int beginPosition = token.beginPosition();
					int endPosition = token.endPosition();
					for (int i = lastEndPosition; i < beginPosition; i++) {
						sb.append(" ");
					}
					lastEndPosition = endPosition;

					// this is the original text of the token
					String origText = token
							.get(CoreAnnotations.OriginalTextAnnotation.class);
					// this is the text of the token
					String word = token.get(TextAnnotation.class);
					// this is the POS tag of the token
					String pos = token.get(PartOfSpeechAnnotation.class);
					// this is the NER label of the token
					String ne = token.get(NamedEntityTagAnnotation.class);

					if ((ne.equals("PERSON") || ne.equals("LOCATION"))
							&& !whiteListMap.containsKey(token.toString().toLowerCase())) {
						if (ne.equals("PERSON")) {
							sb.append("[PERSON]");
						} else /* if( ne.equals("LOCATION") ) */{
							sb.append("[LOCATION]");
						}
						// else if( ne.equals("ORGANIZATION") ) {
						// sb.append("ORGANIZATION");
						// }
					} else {
						sb.append(origText);
					}

					// System.out.println(origText + " | " + word + " | " + pos
					// + " | " + ne);

					// // this is the parse tree of the current sentence
					// Tree tree = sentence.get(TreeAnnotation.class);
					//
					// // this is the Stanford dependency graph of the current
					// sentence
					// SemanticGraph dependencies =
					// sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
				}
				sentenceList.add(sb.toString() + " ");
			}
		}

		StringBuffer sb2 = new StringBuffer();
		for (String s : sentenceList) {
			sb2.append(s);
			sb2.append(" ");
		}

		// This is the coreference link graph
		// Each chain stores a set of mentions that link to each other,
		// along with a method for getting the most representative mention
		// Both sentence and token offsets start at 1!
		// Map<Integer, CorefChain> graph =
		// annotation.get(CorefChainAnnotation.class);

		return sb2.toString();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		// creates a StanfordCoreNLP object, with POS tagging, lemmatization,
		// NER, parsing, and coreference resolution

		NamedEntityRecognitionMCW namedEntityRecognition = new NamedEntityRecognitionMCW();

		System.out.println("Enter text to parse (or return to exit): ");

		// open up standard input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		String text = "Jay Urbain sent an email to Stanford University. He didn't get a reply.";
		while ((text = br.readLine()).length() > 0) {

			String outputText = namedEntityRecognition.performAnnotation(text);
			System.out.println("***");
			System.out.println(outputText);
		}
	}
}
