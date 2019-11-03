package cecs429.query;

import cecs429.index.Index;
import cecs429.index.Posting;
import java.util.ArrayList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An AndQuery composes other QueryComponents and merges their postings in an
 * intersection-like operation.
 */
public class AndQuery implements QueryComponent {

    private List<QueryComponent> mComponents;

    public AndQuery(List<QueryComponent> components) {
        mComponents = components;
    }

    @Override
    public List<Posting> getPostings(Index[] index) {
        List<Posting> result = new ArrayList<>();

        List<QueryComponent> NegativeComponents = new ArrayList<>();

        for (QueryComponent qc : mComponents) {

            if (qc.gettype()) {
                if (result.isEmpty()) {
                    result = qc.getPostings(index);
                    continue;
                }
                List<Posting> posting = qc.getPostings(index);

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
                            j++;
                        } else {
                            i++;
                        }
                    }
                }
            } else {
                NegativeComponents.add(qc);
            }
        }
        for (QueryComponent qc : NegativeComponents) {
            List<Posting> posting = qc.getPostings(index);
            List<Posting> temp = new ArrayList<Posting>(result);
            result.clear();
            int i = 0, j = 0;
            while (i < temp.size() && j < posting.size()) {
                if (temp.get(i).getDocumentId() == posting.get(j).getDocumentId()) {
                    i++;
                    j++;
                } else {
                    if (temp.get(i).getDocumentId() > posting.get(j).getDocumentId()) {
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

        }
        // TODO: program the merge for an AndQuery, by gathering the postings of the composed QueryComponents and
        // intersecting the resulting postings.

        return result;
    }

    @Override
    public String toString() {
        return String.join(" ", mComponents.stream().map(c -> c.toString()).collect(Collectors.toList()));
    }

    @Override
    public boolean gettype() {
        return true;
    }
}
