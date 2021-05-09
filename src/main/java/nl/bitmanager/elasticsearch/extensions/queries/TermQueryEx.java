package nl.bitmanager.elasticsearch.extensions.queries;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;

public class TermQueryEx extends TermQuery {
    private final float boost;
    public final String parentTerm;
    public TermQueryEx(Term t, String parentTerm, float weight) {
        super(t);
        this.boost = weight;
        this.parentTerm = parentTerm;
//        org.apache.lucene.search.FuzzyQuery x;
//        org.apache.lucene.search.DisjunctionMaxQuery x;
    }
    
    
    
    /** Returns true iff <code>other</code> is equal to <code>this</code>. */
    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        TermQueryEx otherTQ = (TermQueryEx)other;
        return parentTerm == otherTQ.parentTerm && boost == otherTQ.boost;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ 17*Objects.hash(parentTerm, boost);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight sub = super.createWeight(searcher, scoreMode, boost * this.boost);
        return parentTerm==null || parentTerm.length()==0 ? sub : new _Weight(sub, this);
    }


    public static class _Weight extends Weight {
        private final Weight sub;
        private final TermQueryEx query;
        public _Weight(Weight sub, TermQueryEx parent) {
            super(parent);
            this.sub = sub;
            this.query = parent;
        }
        
        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            return sub.bulkScorer(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return sub.isCacheable(ctx);
        }
        @Override
        public void extractTerms(Set<Term> terms) {
            sub.extractTerms(terms);
        }
        
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation subEx = sub.explain(context, doc);
            if (subEx.isMatch()) {
                return Explanation.match (subEx.getValue(),
                        this.query.parentTerm + "/" + subEx.getDescription(),
                        subEx.getDetails()); 
            }
            return subEx;
        }
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            return sub.scorer(context);
        }
        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return sub.matches(context, doc);
        }

    }
}
