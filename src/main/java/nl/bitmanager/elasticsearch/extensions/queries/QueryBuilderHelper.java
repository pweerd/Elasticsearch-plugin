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

package nl.bitmanager.elasticsearch.extensions.queries;

import java.io.IOException;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class QueryBuilderHelper {
    
    protected static void throwUnexpectedToken (XContentParser parser, String name) throws IOException {
        throw new ParsingException(parser.getTokenLocation(),
                        String.format("Unexpected token %s in [%s] query.", parser.currentToken(), name));
    }
    
    protected static void throwUnsupportedField (XContentParser parser, String name) throws IOException {
         throw new ParsingException(parser.getTokenLocation(),
                            String.format("Query [%s] does not support [%s]", name, parser.currentName()));
    }
}
