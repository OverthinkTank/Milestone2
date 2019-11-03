/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.AdvancedTokenProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author dhrum
 */
public class NearLiteral implements QueryComponent {

    private List<String> mTerms = new ArrayList<>();
    private int near_k;

    /**
     * Constructs a NearLiteral with the given individual phrase terms.
     */
    public NearLiteral(List<String> terms) {
        AdvancedTokenProcessor processor = new AdvancedTokenProcessor();
        List<String> s = new ArrayList(processor.processToken(terms.get(0)));
        mTerms.add(s.get(s.size() - 1));
        List<String> s1 = new ArrayList(processor.processToken(terms.get(2)));
        mTerms.add(s1.get(s1.size() - 1));

        //mTerms.add(terms.get(2));
        near_k = terms.get(1).charAt(5);
    }

    /**
     * Constructs a NearLiteral given a string with one or more individual terms
     * separated by Near/k term.
     */
    public NearLiteral(String terms) {
        List<String> terms_list = Arrays.asList(terms.split(" "));
        AdvancedTokenProcessor processor = new AdvancedTokenProcessor();
        List<String> s = new ArrayList(processor.processToken(terms_list.get(0)));
        mTerms.add(s.get(s.size() - 1));
        List<String> s1 = new ArrayList(processor.processToken(terms_list.get(2)));
        mTerms.add(s1.get(s1.size() - 1));

        near_k = Integer.parseInt(terms_list.get(1).charAt(5) + "");

    }

    @Override
    public List<Posting> getPostings(Index[] index) {
        String firstterm = mTerms.get(0);
        List<Posting> result = index[0].getPostings(firstterm);
        //    for (String term : mTerms) {
        //   if (term.equals(firstterm)) {
        //    continue;
        //  }
        String second_term = mTerms.get(1);
        List<Posting> posting = index[0].getPostings(second_term);
        List<Posting> temp = new ArrayList<>(result);
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
                    if ((position2 - position1) <= near_k && position2 > position1) {

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

//        }
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
