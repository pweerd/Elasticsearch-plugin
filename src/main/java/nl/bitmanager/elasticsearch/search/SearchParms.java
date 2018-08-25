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

package nl.bitmanager.elasticsearch.search;

import java.io.IOException;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.plugins.SearchPlugin.SearchExtSpec;
import org.elasticsearch.search.SearchExtBuilder;

/**
 * Extension class to be able to supplu extra search parameters
 * Accessible via ext: { _bm: {}} in the query-body
 */
public class SearchParms extends SearchExtBuilder {
    public static final String WRITEABLENAME = "_bm";
    public static final String F_DIAGNOSTICS = "diagnostics";
    
    public final boolean diagnostics;

    public SearchParms(StreamInput in) throws IOException {
        diagnostics = in.readBoolean(); 
    }

    public SearchParms(XContentParser parser) throws IOException {
        Token token;
        String fieldName=null;
        boolean diagnostics = true;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
            case FIELD_NAME: 
                fieldName = parser.currentName();
                continue;
            case VALUE_BOOLEAN:
                if (F_DIAGNOSTICS.equals(fieldName)) {
                    diagnostics = parser.booleanValue();
                    continue;
                }
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + WRITEABLENAME + "] object.");
            }
            throw new ParsingException(parser.getTokenLocation(),
                         "Unknown key for a " + token + " in [" + WRITEABLENAME + "]: [" + fieldName + "].");
        }
        this.diagnostics = diagnostics;
    }

    @Override
    public String getWriteableName() {
        return WRITEABLENAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(diagnostics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(WRITEABLENAME);
        builder.field(F_DIAGNOSTICS, diagnostics);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return diagnostics ? 1 : 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj==null || getClass() != obj.getClass()) return false;
        return diagnostics == ((SearchParms)obj).diagnostics;
    }

    public static SearchExtSpec<SearchParms> createSpec() {
        return new SearchExtSpec<SearchParms>(WRITEABLENAME, 
                SearchParms::read, 
                SearchParms::parse);
    }
    
    private static SearchParms parse (XContentParser parser) throws IOException {
        return new SearchParms(parser);
    }
    
    private static SearchParms read (StreamInput in) throws IOException {
        return new SearchParms(in);
    }
}
