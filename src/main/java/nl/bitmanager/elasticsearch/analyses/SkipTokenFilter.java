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

package nl.bitmanager.elasticsearch.analyses;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

/**
 * This filter skips either the first N tokens (min_position&lt;0), or it wait
 * for the first token at the Nth position.
 */
public class SkipTokenFilter extends TokenFilter {
    private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);

    private final int minPosition;
    private final int toSkip;
    private int pos;
    private int curToSkip;
    private int state;

    public SkipTokenFilter(TokenStream in, int minPosition) {
        super(in);
        int _skip = 0;
        int _minpos = 0;
        if (minPosition < 0)
            _skip = -minPosition;
        else
            _minpos = minPosition;
        this.minPosition = _minpos;
        this.toSkip = _skip;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        while (input.incrementToken()) {
            switch (state) {
            case 1:
                if (posIncAttribute.getPositionIncrement() == 0)
                    continue;
                state = 2;
                return true;
            case 2:
                return true;
            default:
                break;
            }

            if (curToSkip > 0) {
                if (0 == --curToSkip)
                    state = 1;
                continue;
            }

            pos += posIncAttribute.getPositionIncrement();
            if (pos < minPosition)
                continue;
            state = 2;
            return true;
        }
        return false;
    }

    public void reset() throws IOException {
        super.reset();
        curToSkip = toSkip;
        pos = 0;
        state = 0;
    }

    public static class Factory extends AbstractTokenFilterFactory {

        private final int minPosition;

        public Factory(IndexSettings indexSettings, String name, Settings settings) {
            super(indexSettings, name, settings);
            this.minPosition = settings.getAsInt("min_position", 2);
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new SkipTokenFilter(tokenStream, minPosition);
        }
    }

}
