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
import java.util.Locale;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Explanation;

public class BoundedScorerNorms extends BoundedScorer {
    private NumericDocValues norms;
    protected final float weight_idf;
    protected final float weight_boost;

    public BoundedScorerNorms(BoundedWeight weight, NumericDocValues norms)
    {
        super (weight);
        this.norms = norms;
        weight_idf = weight.idf;
        weight_boost = weight.queryBoost;
    }
    
    public float scoreTf (int docLen, float freq) {
        if (forceTf > 0) freq = forceTf;
        else if (freq > 255f) freq = 255f;
        return (float)(maxTf * Math.log(biasTf+freq) / Math.log(biasTf + docLen));

    }

    @Override
    public float score(int doc, float freq) throws IOException {
        if (!norms.advanceExact(doc)) return 1.0f;
        
        int tf = forceTf;
        if (tf <= 0) tf = (int)freq;
        if (tf > 255) tf = 255;
        
        int fl = (int)norms.longValue();
        return (float)(weight_boost*(weight_idf + maxTf * Math.log(biasTf+tf) / Math.log(biasTf + fl)));
    }
    
    @Override
    public Explanation explain(int doc, Explanation freq) throws IOException {
        if (!norms.advanceExact(doc)) return super.explain(doc, freq);
        
        int tf = forceTf;
        if (tf <= 0) tf = (int)freq.getValue();
        if (tf > 255) tf = 255;
        
        int fl = (int)norms.longValue();
        float tfBoost = (float)(maxTf * Math.log(biasTf+tf) / Math.log(biasTf + fl));

        String msg = String.format (Locale.ROOT, "tfBoost (tf=%.2f [%.2f], fieldlen=%d, maxTf=%.2f, forceTf=%d, bias=%.2f)", tf, freq.getValue(), fl, maxTf, forceTf, biasTf);
        return weight.createExplain(doc, Explanation.match(tfBoost, msg, BoundedWeight.EMPTY_EXPLAIN_LIST));
    }



}
