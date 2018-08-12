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

package nl.bitmanager.elasticsearch.similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity.SimWeight;

public class BoundedWeight extends SimWeight {
    public final BoundedSimilarity parent;
    public final BoundedSimilaritySettings settings;
    public final String field;
    private long totalDocs, maxDocFreq; 
    public final float maxIdf; 
    public final float queryBoost;
    public float idf;
    public float normScore, norm;
    
//float boost, CollectionStatistics collectionStats, TermStatistics... termStats
    
    public BoundedWeight (BoundedSimilarity sim, float boost, CollectionStatistics collectionStats, TermStatistics[] termStats) {
        this.parent = sim;
        this.settings = sim.settings;
        this.queryBoost = boost;
        
        this.maxIdf = settings.maxIdf; 
        this.totalDocs = collectionStats.maxDoc();
        this.field = collectionStats.field();
        
        maxDocFreq=0;
        if (maxIdf == 0) {
           idf = 1.0f;
        }
        else {
           for (int i = termStats.length - 1; i >= 0; i--) {
              long df = termStats[i].docFreq();
              if (df > maxDocFreq)
                 maxDocFreq = df;
           }
           double max = Math.log(1.0 + (totalDocs - 0.5D) / (1.5D));
           idf = (float) (1.0 + maxIdf * Math.log(1.0 + (totalDocs - maxDocFreq + 0.5D) / (maxDocFreq + 0.5D)) / max);
        }
        norm = 1.0f;
    }
    
    public BoundedScorer createScorer(LeafReaderContext context) throws IOException {
        NumericDocValues norms = settings.maxTf==0.0f ? null : context.reader().getNormValues(field);
        return (norms==null) ? new BoundedScorer (this) : new BoundedScorerNorms(this, norms);
    }
    

    public float getScore (float tfBoost) {
        return queryBoost * (idf + tfBoost);
    }

    public static final List<Explanation> EMPTY_EXPLAIN_LIST  = new ArrayList<Explanation>(0);
    public Explanation createExplain (int doc, Explanation tfExplain) {
        List<Explanation> subs = new ArrayList<Explanation>(4);
        if (queryBoost != 1.0f) {
            subs.add(Explanation.match (normScore, "boost (queryBoost=" + queryBoost + ")", EMPTY_EXPLAIN_LIST));
         }
        if (idf != 1.0f) {
            subs.add(Explanation.match (idf, "idf(docFreq=" + maxDocFreq + ", maxDocs=" + totalDocs + ", maxIdf=" + maxIdf + ")", EMPTY_EXPLAIN_LIST));
        }
        
        float tfScore;
        if (tfExplain == null)
            tfScore = 0.0f;
        else {
            tfScore = tfExplain.getValue();
            subs.add (tfExplain);
        }
        
        return Explanation.match (getScore (tfScore), "score(doc=" + doc + "), computed from:", subs);
    }
 }
