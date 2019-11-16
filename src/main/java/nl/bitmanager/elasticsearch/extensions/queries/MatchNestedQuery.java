/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.bitmanager.elasticsearch.extensions.queries;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.lucene.search.Queries;

/** Sole purpose of this query is to make sure that we don't get automatically filtered by nested parents.
 *  So, the inner query is allowed to produce nested records, which is particularly handy
 *  when doing aggregations over nested records, while not caring about the hits anyway.
 *  WARNING: the hits are nested records, not the normal records.
 *
 *  This object has TermQuery is a superclass, because that class is checked by ES to see if a query can
 *  produce nested records. If it is 'guaranteed' that a query cannot produce nested records, no automatic filtering is applied.
 */
public class MatchNestedQuery extends TermQuery {
    private final Query wrapped;
    private final Query rewritten;
    private final static Term dummy = new Term ("!a_", "!a_");
    public MatchNestedQuery (Query q) {
        this(q, null);
    }
    private MatchNestedQuery (Query q, Query rewritten) {
        super (dummy);
        this.wrapped = q;
        this.rewritten = rewritten;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return rewritten.createWeight(searcher, scoreMode, boost);
    }

    /** Prints a user-readable version of this query. */
    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append(AllowNestedQueryBuilder.NAME);
        sb.append('(');
        sb.append(wrapped.toString());
        sb.append(')');
        return sb.toString();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewritten != null) return this;

        Query newWrapped;
        if (wrapped==null)
            newWrapped = Queries.newNestedFilter().rewrite(reader);
        else {
            Builder bldr = new BooleanQuery.Builder();
            bldr.add(Queries.newNestedFilter(), Occur.FILTER);
            bldr.add(wrapped, Occur.MUST);
            newWrapped =  bldr.build().rewrite(reader);
        }
        return new MatchNestedQuery(wrapped, newWrapped);
    }

    /** Returns true iff <code>o</code> is equal to this. */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
                Objects.equals(wrapped, ((MatchNestedQuery)other).wrapped) &&
                Objects.equals(rewritten, ((MatchNestedQuery)other).rewritten);
    }

    @Override
    public int hashCode() {
      return Objects.hash(wrapped, rewritten);
    }

}
