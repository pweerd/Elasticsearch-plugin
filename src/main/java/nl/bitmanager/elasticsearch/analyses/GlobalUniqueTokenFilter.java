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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

/**
 * A GlobalUniqueTokenFilter acts like a normal UniqueTokenFilter, but its
 * doesn't reset the internal administration on a reset() The reason is that
 * reset() is called between tokenization of array-elements if any, so when ES
 * indexes an array of strings, the normal UniqueTokenFilter will emit duplicate
 * tokens: it only deduplicates tokens within the same array-element. By not
 * resetting the administration, the GlobalUniqueTokenFilter de-deplicates over
 * complete arrays. Note that this might be a problem when ES decides to re-use
 * token streams for multiple index fields!
 */
public class GlobalUniqueTokenFilter extends TokenFilter {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);

    // use a fixed version, as we don't care about case sensitivity.
    private final CharArraySet previous = new CharArraySet(8, false);
    private final boolean onlyOnSamePosition;

    public GlobalUniqueTokenFilter(TokenStream in) {
        super(in);
        this.onlyOnSamePosition = false;
    }

    public GlobalUniqueTokenFilter(TokenStream in, boolean onlyOnSamePosition) {
        super(in);
        this.onlyOnSamePosition = onlyOnSamePosition;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        while (input.incrementToken()) {
            final char term[] = termAttribute.buffer();
            final int length = termAttribute.length();

            boolean duplicate;
            if (onlyOnSamePosition) {
                final int posIncrement = posIncAttribute.getPositionIncrement();
                if (posIncrement > 0) {
                    previous.clear();
                }

                duplicate = (posIncrement == 0 && previous.contains(term, 0, length));
            } else {
                duplicate = previous.contains(term, 0, length);
            }

            // clone the term, and add to the set of seen terms.
            char saved[] = new char[length];
            System.arraycopy(term, 0, saved, 0, length);
            previous.add(saved);

            if (!duplicate) {
                return true;
            }
        }
        return false;
    }

    // PW: we don't override reset, since we don't want to clear our 'previous'
    // collection
    // public void reset() throws IOException {
    // Utils.printStackTrace("reset");
    // super.reset();
    // }

    public static class Factory extends AbstractTokenFilterFactory {

        private final boolean onlyOnSamePosition;

        public Factory(IndexSettings indexSettings, String name, Settings settings) {
            super(indexSettings, name, settings);
            this.onlyOnSamePosition = settings.getAsBoolean("only_on_same_position", false);
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new GlobalUniqueTokenFilter(tokenStream, onlyOnSamePosition);
        }
    }

}
