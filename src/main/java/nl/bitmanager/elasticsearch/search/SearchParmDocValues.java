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

public class SearchParmDocValues extends SearchExtBuilder {
    public static final String WRITEABLENAME = "_bm";
    public static final String F_DOCVALUES = "docvalues";
    
    public final boolean docvalues;

    public SearchParmDocValues(StreamInput in) throws IOException {
        docvalues = (in.readByte() == (byte)'T'); 
    }

    public SearchParmDocValues(XContentParser parser) throws IOException {
        Token token;
        String fieldName=null;
        boolean docvals = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            System.out.println("next token: " + token);
            switch (token) {
            case FIELD_NAME: 
                fieldName = parser.currentName();
                continue;
            case VALUE_BOOLEAN:
                if (F_DOCVALUES.equals(fieldName)) {
                    docvals = parser.booleanValue();
                    continue;
                }
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + WRITEABLENAME + "] object.");
            }
            throw new ParsingException(parser.getTokenLocation(),
                         "Unknown key for a " + token + " in [" + WRITEABLENAME + "]: [" + fieldName + "].");
        }
        this.docvalues = docvals;
    }

    @Override
    public String getWriteableName() {
        return WRITEABLENAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) (docvalues ? 'T' : 'F'));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(WRITEABLENAME);
        builder.field(F_DOCVALUES, docvalues);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return docvalues ? 'T' : 'F';
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    public static SearchExtSpec<SearchParmDocValues> createSpec() {
        return new SearchExtSpec<SearchParmDocValues>(WRITEABLENAME, 
                SearchParmDocValues::read, 
                SearchParmDocValues::parse);
    }
    
    private static SearchParmDocValues parse (XContentParser parser) throws IOException {
        return new SearchParmDocValues(parser);
    }
    
    private static SearchParmDocValues read (StreamInput in) throws IOException {
        return new SearchParmDocValues(in);
    }
}
