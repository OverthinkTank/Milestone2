package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import java.util.ArrayList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An OrQuery composes other QueryComponents and merges their postings with a
 * union-type operation.
 */
public class OrQuery implements QueryComponent {
    // The components of the Or query.

    private List<QueryComponent> mComponents;

    public OrQuery(List<QueryComponent> components) {
        mComponents = components;
    }

    @Override
    public List<Posting> getPostings(Index[] index) {
        List<Posting> result = new ArrayList();

        for (QueryComponent qc : mComponents) {
            List<Posting> posting = qc.getPostings(index);
            if (result.isEmpty()) {
                result.addAll(posting);
                continue;
            }
            List<Posting> temp = new ArrayList<Posting>(result);

            result.clear();
            int i = 0, j = 0;

            while (i < temp.size() && j < posting.size()) {
                if (temp.get(i).getDocumentId() == posting.get(j).getDocumentId()) {
                    result.add(temp.get(i));

                    i++;
                    j++;
                } else {
                    if (temp.get(i).getDocumentId() > posting.get(j).getDocumentId()) {
                        result.add(posting.get(j));
                        j++;
                    } else {
                        result.add(temp.get(i));
                        i++;
                    }
                }
            }
            while (i < temp.size()) {
                result.add(temp.get(i));
                i++;
            }
            while (j < posting.size()) {
                result.add(posting.get(j));
                j++;
            }

        }
        // TODO: program the merge for an OrQuery, by gathering the postings of the composed QueryComponents and
        // unioning the resulting postings.
        return result;
    }

    @Override
    public String toString() {
        // Returns a string of the form "[SUBQUERY] + [SUBQUERY] + [SUBQUERY]"
        return "("
                + String.join(" + ", mComponents.stream().map(c -> c.toString()).collect(Collectors.toList()))
                + " )";
    }

    @Override
    public boolean gettype() {
        return true;
    }
}
