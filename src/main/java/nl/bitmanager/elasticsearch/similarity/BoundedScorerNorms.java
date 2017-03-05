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

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Explanation;

public class BoundedScorerNorms extends BoundedScorer {
    private NumericDocValues norms;
    protected final float weight_normScore;
    protected final float weight_idf;


    public BoundedScorerNorms(BoundedWeight weight, NumericDocValues norms)
    {
        super (weight);
        this.norms = norms;
        weight_normScore = weight.normScore;
        weight_idf = weight.idf;
    }
    
    public float scoreTf (int docLen, float freq) {
        if (forceTf > 0) freq = forceTf;
        else if (freq > 255f) freq = 255f;
        return (float)(maxTf * Math.log(biasTf+freq) / Math.log(biasTf + docLen));

    }

    @Override
    public float score(int doc, float freq) {
        return weight_normScore * (weight_idf + scoreTf ((int)norms.get(doc), freq));
    }
    
    @Override
    public Explanation explain(int doc, Explanation freq) {
        float tf = freq.getValue();

        int doclen = (int)norms.get(doc);
        if (forceTf > 0) tf = forceTf;
        float tfBoost = scoreTf (doclen, tf);
        
        String msg = String.format (Locale.ROOT, "tfBoost (tf=%.2f [%.2f], fieldlen=%d, maxTf=%.2f, forceTf=%d, bias=%.2f)", tf, freq.getValue(), doclen, maxTf, forceTf, biasTf);
        return weight.createExplain(doc, Explanation.match(tfBoost, msg, BoundedWeight.EMPTY_EXPLAIN_LIST));
    }



}
