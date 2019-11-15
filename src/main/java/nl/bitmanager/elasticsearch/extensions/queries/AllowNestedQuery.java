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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;

/** Sole purpose of this query is to make sure that we don't get automatically filtered by nested parents.
 *  So, the inner query is allowed to produce nested records, which is particularly handy 
 *  when doing aggregations over nested records, while not caring about the hits anyway.
 *  WARNING: the hits are nested records, not the normal records.
 *  
 *  This object has TermQuery is a superclass, because that class is checked by ES to see if a query can
 *  produce nested records. If it is 'guaranteed' that a query cannot produce nested records, no automatic filtering is applied.
 */
public class AllowNestedQuery extends TermQuery {
    private final Query wrapped;
    private final static Term dummy = new Term ("!a_", "!a_");
    public AllowNestedQuery (Query q) {
        super (dummy);
        wrapped = q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return wrapped.createWeight(searcher, scoreMode, boost);
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
        Query qw = wrapped.rewrite(reader);
        return qw==wrapped ? this : new AllowNestedQuery (qw);
      }


    /** Returns true iff <code>o</code> is equal to this. */
    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && wrapped.equals(((AllowNestedQuery) other).wrapped);
    }

    @Override
    public int hashCode() {
      return wrapped.hashCode();
    }

}
