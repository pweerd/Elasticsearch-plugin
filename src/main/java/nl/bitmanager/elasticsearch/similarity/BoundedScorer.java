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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

public class BoundedScorer extends SimScorer {
    private final Explanation idfExplain;

    private final float weight_idf;
    private final float weight_boost;

    private final float maxTf;
    private final float biasTf;
    private final float forceTf;


    public BoundedScorer(BoundedSimilarity parent, Explanation idfExplain, float boost, float idf)
    {
        final BoundedSimilaritySettings settings = parent.settings;
        this.idfExplain = idfExplain;
        maxTf = settings.maxTf;
        biasTf = settings.biasTf;
        forceTf = settings.forceTf;
        weight_idf = idf;
        weight_boost = boost;
    }

    public float scoreTf (int docLen, float freq) {
        if (forceTf > 0) freq = forceTf;
        else if (freq > 255f) freq = 255f;
        return (float)(maxTf * Math.log(biasTf+freq) / Math.log(biasTf + docLen));

    }

    private float score_tf(float freq, long norm) {
        float tf = (forceTf > 0) ? (float)forceTf : freq;
        if (tf > 255.0f) tf = 255.0f;

        float fnorm = (float)norm;
        if (tf>fnorm) tf = fnorm;

        return (float)(maxTf * Math.log(biasTf+tf) / Math.log(biasTf + fnorm));
    }

    @Override
    public float score(float freq, long norm) {
        return (float)(weight_boost*(weight_idf + maxTf * score_tf(freq, norm)));
    }


    @Override
    public Explanation explain(Explanation freq, long norm) {
        float tfBoost = maxTf * score_tf((float)freq.getValue(), norm);
        float score = weight_boost*(weight_idf+tfBoost);

        List<Explanation> sub = new ArrayList<Explanation>();
        String msg = String.format (Locale.ROOT, "tfBoost (tf=%.1f, fieldlen=%d, maxTf=%.2f, forceTf=%d, bias=%.2f)", freq.getValue(), norm, maxTf, forceTf, biasTf);
        sub.add (idfExplain);
        sub.add (Explanation.match (tfBoost, msg));
        return Explanation.match (score, "", sub);
    }



}
