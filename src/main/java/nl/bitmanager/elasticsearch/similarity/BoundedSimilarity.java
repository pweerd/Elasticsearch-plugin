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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.similarity.AbstractSimilarityProvider;

public class BoundedSimilarity extends Similarity {
    public BoundedSimilaritySettings settings;

    public BoundedSimilarity(BoundedSimilaritySettings settings) {
        this.settings = settings;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
        BoundedSimilaritySettings settings = this.settings;
        return settings.discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength();
    }

    @Override
    public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return new BoundedWeight(this, boost, collectionStats, termStats);
    }

    @Override
    public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
        return ((BoundedWeight) weight).createScorer(context);
    }

    public static class Provider extends AbstractSimilarityProvider {
        private BoundedSimilarity similarity;
        private BoundedSimilaritySettings settings;

        public Provider(String name, Settings providerSettings, Settings indexSettings) {
            super(name);
            this.settings = new BoundedSimilaritySettings(providerSettings);
            similarity = new BoundedSimilarity(this.settings);
            // System.out.println("Loaded: " + toString());
        }

        public Similarity get() {
            return similarity;
        }

        public String toString() {
            return getClass().getSimpleName() + " (" + similarity.toString() + ")";
        }
    }

}
