package nl.bitmanager.elasticsearch.extensions.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

public class TermQueryEx extends TermQuery {
    private final float boost;
    public final String parentTerm;
    public TermQueryEx(Term t, String parentTerm, float weight) {
        super(t);
        this.boost = weight;
        this.parentTerm = parentTerm;
    }

}
