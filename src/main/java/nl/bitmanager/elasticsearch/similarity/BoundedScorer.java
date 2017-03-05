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

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;

public class BoundedScorer extends SimScorer {
    public final BoundedSimilaritySettings settings;
    public final BoundedWeight weight;

    public final float maxTf;
    public final float biasTf;
    public final int  forceTf;
    
    private final float _score;


    public BoundedScorer(BoundedWeight weight)
    {
        settings = weight.settings;
        this.weight = weight;
        maxTf = settings.maxTf;
        biasTf = settings.biasTf;
        forceTf = settings.forceTf;
        _score = weight.normScore * weight.idf;
    }

    @Override
    public float score(int doc, float freq) {
        return _score;
    }

    private static final Explanation fixed = Explanation.match(0.0f, "tf-boost (norms omitted)", BoundedWeight.EMPTY_EXPLAIN_LIST);
    @Override
    public Explanation explain(int doc, Explanation freq) {
        return weight.createExplain(doc, fixed);
    }

    @Override
    public float computeSlopFactor(int distance) {
        return 1.0f;
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
        return 1.0f;
    }

}
