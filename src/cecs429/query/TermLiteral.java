package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import cecs429.text.AdvancedTokenProcessor;
import java.util.ArrayList;

import java.util.List;

/**
 * A TermLiteral represents a single term in a subquery.
 */
public class TermLiteral implements QueryComponent {

    private String mTerm;

    public TermLiteral(String term) {
        mTerm = term;
        AdvancedTokenProcessor processor = new AdvancedTokenProcessor();
        List<String> s = new ArrayList(processor.processToken(term));
        mTerm = s.get(s.size() - 1);
    }

    public String getTerm() {
        return mTerm;
    }

    @Override
    public List<Posting> getPostings(Index[] index) {
        return index[0].getPostings(mTerm);
    }

    @Override
    public String toString() {
        return mTerm;
    }

    @Override
    public boolean gettype() {
        return true;
    }
}
