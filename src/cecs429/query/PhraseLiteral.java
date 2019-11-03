package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.AdvancedTokenProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a phrase literal consisting of one or more terms that must occur
 * in sequence.
 */
public class PhraseLiteral implements QueryComponent {
    // The list of individual terms in the phrase.

    private List<String> mTerms = new ArrayList<>();

    /**
     * Constructs a PhraseLiteral with the given individual phrase terms.
     */
    public PhraseLiteral(List<String> terms) {
        //mTerms.addAll(terms);
        AdvancedTokenProcessor processor = new AdvancedTokenProcessor();
        for (String term : terms) {
            List<String> s = new ArrayList(processor.processToken(term));
            mTerms.add(s.get(s.size() - 1));
        }

    }

    /**
     * Constructs a PhraseLiteral given a string with one or more individual
     * terms separated by spaces.
     */
    public PhraseLiteral(String terms) {
        List<String> allterms = Arrays.asList(terms.split(" "));
        AdvancedTokenProcessor processor = new AdvancedTokenProcessor();
        for (String term : allterms) {
            List<String> s = new ArrayList(processor.processToken(term));
            mTerms.add(s.get(s.size() - 1));
        }
    }

    @Override
    public List<Posting> getPostings(Index[] index) {
        List<Posting> result = new ArrayList<>();
        if (mTerms.size() == 2) {
            String biword = mTerms.get(0) + " " + mTerms.get(1);
            System.out.println(biword);
            return index[1].getPostings(biword);
        }
        for (String term : mTerms) {
            if (result.isEmpty()) {
                result = index[0].getPostings(term);
                continue;
            }
            List<Posting> posting = index[0].getPostings(term);
            List<Posting> temp = new ArrayList<Posting>(result);
            result.clear();
            int i = 0, j = 0;
            while (i < temp.size() && j < posting.size()) {
                Posting current_posting = temp.get(i);
                Posting new_posting = posting.get(j);
                if (current_posting.getDocumentId() == new_posting.getDocumentId()) {
                    List<Integer> positions_in_current = current_posting.getPositionList();
                    List<Integer> positions_in_new_posting = new_posting.getPositionList();
                    List<Integer> newpostion = new ArrayList<>();
                    int a = 0, b = 0;
                    while (a < positions_in_current.size() && b < positions_in_new_posting.size()) {
                        int position1 = positions_in_current.get(a);
                        int position2 = positions_in_new_posting.get(b);
                        if ((position2 - position1) <= 1 && position2 > position1) {
                            newpostion.add(position2);
                            a++;
                            b++;
                        } else {
                            if (position1 > position2) {
                                b++;
                            } else {
                                a++;
                            }

                        }
                    }
                    if (newpostion.size() > 0) {
                        result.add(new Posting(new_posting.getDocumentId(), newpostion));
                    }
                    i++;
                    j++;
                } else {
                    if (current_posting.getDocumentId() > new_posting.getDocumentId()) {
                        j++;
                    } else {
                        i++;
                    }
                }
            }

        }

        return result;
        // TODO: program this method. Retrieve the postings for the individual terms in the phrase,
        // and positional merge them together.
    }

    @Override
    public String toString() {
        return "\"" + String.join(" ", mTerms) + "\"";
    }

    @Override
    public boolean gettype() {
        return true;
    }
}
