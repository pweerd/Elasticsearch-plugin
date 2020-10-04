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

import java.util.Locale;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;

public class BoundedSimilarity extends Similarity {
    public BoundedSimilaritySettings settings;

    public BoundedSimilarity(BoundedSimilaritySettings settings) {
        this.settings = settings;
    }


    public static Similarity create (Settings settings, Version version, ScriptService scriptService) {
        return new BoundedSimilarity(new BoundedSimilaritySettings(settings));
    }

    @Override
    public long computeNorm(FieldInvertState state) {
        return settings.discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength();
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats,  TermStatistics... termStats) {
        final Explanation idfExplain;
        final float idf;

        if (settings.maxIdf == 0) {
           idf = 1.0f;
           idfExplain = null;
        }
        else {
           long maxDocFreq=0;
           for (int i = termStats.length - 1; i >= 0; i--) {
              long df = termStats[i].docFreq();
              if (df > maxDocFreq)
                 maxDocFreq = df;
           }
           long totalDocs = collectionStats.maxDoc();
           double max = Math.log(1.0 + (totalDocs - 0.5D) / (1.5D));
           idf = (float) (1.0 + settings.maxIdf * Math.log(1.0 + (totalDocs - maxDocFreq + 0.5D) / (maxDocFreq + 0.5D)) / max);
           idfExplain = Explanation.match (idf, String.format (Locale.ROOT, "+= idf=%.3f (docs=%d out of %d, maxIdf=%.3f)", idf, maxDocFreq, totalDocs, settings.maxIdf));
        }
        return new BoundedScorer (this, idfExplain, boost, idf);
    }


}
