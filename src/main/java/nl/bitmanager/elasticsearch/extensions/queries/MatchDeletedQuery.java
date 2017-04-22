package nl.bitmanager.elasticsearch.extensions.queries;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;



public class MatchDeletedQuery extends Query {
    public static final String NAME = "match_deleted";

    protected final Query subQuery;
    
    public MatchDeletedQuery(Query sub) {
        subQuery = sub!=null ? sub : new MatchAllDocsQuery();
    }

    @Override
    public String toString(String field) {
        return String.format("%s (%s)", NAME, subQuery==null ? "" : subQuery.toString(field));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj==null || obj.getClass() != getClass()) return false;
        
        MatchDeletedQuery other = (MatchDeletedQuery)obj;
        return subQuery.equals(other.subQuery);
    }

    @Override
    public int hashCode() {
        return super.classHash() ^ subQuery.hashCode();
    }
    
    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        Weight sub = subQuery.createWeight(searcher, needsScores);
        return new _Weight (this, sub);
    }
    
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = subQuery.rewrite(reader);
        return rewritten==subQuery ? this : new MatchDeletedQuery(rewritten);
    }

    
    protected static class _Weight extends Weight {
        protected final Query subQuery;
        protected final Weight subWeight;

        protected _Weight(MatchDeletedQuery query, Weight subWeight) {
            super(query);
            this.subQuery = query.subQuery;
            this.subWeight = subWeight;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            subWeight.extractTerms(terms);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return subWeight.explain(context, doc);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return subWeight.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float boost) {
            subWeight.normalize(norm, boost);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            return new _BulkScorer (context, subWeight.bulkScorer(context)) ;
        }
        
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            return subWeight.scorer(context);
        }

    }

    protected static class _BulkScorer extends BulkScorer {
        protected final BulkScorer sub;
        protected Bits prevBits, newBits;
        protected int numDocs; 

        public _BulkScorer (LeafReaderContext context, BulkScorer sub) {
            this.sub = sub;
            numDocs = context.reader().numDocs();
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            //System.out.printf("score: accept=%s, min=%d, max=%d, docs=%d\n", _tos(acceptDocs), min, max, numDocs);
            if (acceptDocs==null) return DocIdSetIterator.NO_MORE_DOCS;
            
            if (acceptDocs == prevBits)
                acceptDocs = newBits;
            else {
                acceptDocs = newBits = new _NotBits (acceptDocs, 0);
                prevBits = acceptDocs;
            }
                
            return sub.score (collector, acceptDocs, min, max);
        }

        @Override
        public long cost() {
            return sub.cost();
        }
        
//        private String _tos(Bits bits) {
//            if (bits==null ) return "null";
//            return String.format("Bits(cls=%s, len=%d", bits.getClass(), bits.length());
//        }
    }
    
    protected static class _NotBits implements Bits {
        protected final Bits sub;
        protected final int len;
        
        public _NotBits(Bits sub, int len) {
            this.sub = sub;
            this.len = len;
        }

        @Override
        public boolean get(int index) {
            return !sub.get(index);
        }

        @Override
        public int length() {
            return sub.length();
        }
    }
}
