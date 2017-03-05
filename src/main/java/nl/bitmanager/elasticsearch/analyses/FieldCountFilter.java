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
import java.lang.reflect.Field;

import nl.bitmanager.elasticsearch.extensions.Plugin;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeSource;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

/**
 * A FieldCountFilter works in conjunction with the similarity. It supports hand
 * crafted field length to be store in the index (as a norm) This filter can
 * work in 2 modi: 1) (prefix==null) In this mode the tokens are just counted
 * and stored 2) (prefix != null) In this mode the filter searches for a token
 * in the form of [prefix][number], where number is interpreted as the field
 * length. If no such token is found, a negative value for field length will be
 * returned.
 */

public class FieldCountFilter extends TokenFilter {
    private final CharTermAttribute termAttribute;
    private final String countPrefix;
    private final int prefixLen;
    private int fieldLen;

    public int getFieldLen() {
        return fieldLen;
    }

    public FieldCountFilter(TokenStream in, String countPrefix) {
        super(in);
        this.termAttribute = addAttribute(CharTermAttribute.class);
        this.countPrefix = countPrefix;
        if (countPrefix == null) {
            this.prefixLen = 0;
            this.fieldLen = 0;
        } else {
            this.prefixLen = countPrefix.length();
            this.fieldLen = -1;
        }
    }

    @Override
    public void reset() throws IOException {
        fieldLen = prefixLen == 0 ? 0 : -1; // also reset stored field length
        super.reset();
    }

    @Override
    public final boolean incrementToken() throws IOException {
        while (input.incrementToken()) {
            // If no prefix specified: just count the # tokens
            if (prefixLen == 0) {
                fieldLen++;
                return true;
            }

            // Check if this token could be a length-token. If not: early out
            final int length = termAttribute.length();
            if (length <= prefixLen)
                return true;
            final char term[] = termAttribute.buffer();

            int i;
            for (i = 0; i < prefixLen; i++) {
                if (term[i] != countPrefix.charAt(i))
                    break;
            }
            if (i < prefixLen)
                return true;

            // The prefix is OK, check the number. Just return the token if we
            // cannot interpret the integer part
            int len = 0;
            for (; i < length; i++) {
                if (term[i] < '0' || term[i] > '9')
                    return true;
                len = 10 * len + (int) term[i] - (int) '0';
            }

            // Store the field length and eat the token
            fieldLen = len;
        }
        return false;
    }

    private static AttributeSource getParent(AttributeSource obj) throws Exception {
        try {
            Class<?> cls = obj.getClass();
            while (cls != null) {
                // System.out.println("-- checking class=" + cls.getName());
                if (cls == TokenFilter.class) {
                    Field f = cls.getDeclaredField("input"); // NoSuchFieldException
                    f.setAccessible(true);
                    return (AttributeSource) f.get(obj);
                }
                cls = cls.getSuperclass();
            }
            throw new NoSuchFieldException("input");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Search for the 1st FieldCountFilter in the chain of TokenFilters If one
     * is found, that stored fieldLen is used, otherwise the supplied default is
     * returned. Exceptions are just logged
     */
    public static int GetFieldLength(AttributeSource attributeSource, int def) {
        // System.out.println();
        // System.out.println("GetFieldLength");
        try {
            AttributeSource src = attributeSource;
            while (true) {
                // System.out.println("-- type=" + src.getClass().getName());
                if (src instanceof FieldCountFilter) {
                    int len = ((FieldCountFilter) src).fieldLen;
                    // System.out.println("-- fl=" + len + ", def=" + def);
                    return len < 0 ? def : len;
                }
                if ((src instanceof TokenFilter)) {
                    src = getParent(src);
                    continue;
                }
                // System.out.println("-- -- not a tokenFilter");
                break;
            }
        } catch (Exception err) {
            // System.out.println(err);
            Plugin.logger.error("Cannot fetch fieldlength: " + err.getMessage(), err);
        }
        return def;
    }

    public static class Factory extends AbstractTokenFilterFactory {

        private final String countPrefix;

        public Factory(IndexSettings indexSettings, String name, Settings settings) {
            super(indexSettings, name, settings);
            this.countPrefix = settings.get("count_prefix");
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new FieldCountFilter(tokenStream, countPrefix);
        }
    }
}
