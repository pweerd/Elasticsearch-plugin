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

package nl.bitmanager.elasticsearch.extensions.termlist;

import java.io.IOException;
import java.util.ArrayList;

import org.elasticsearch.common.xcontent.XContentBuilder;

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class Collisions extends ArrayList<CollisionElt> {
    private static final long serialVersionUID = -4323224480015734933L;

    public void exportToJson(XContentBuilder builder, TermlistTransportItem request, TypeHandler th) throws IOException {
        int N = this.size();
        int limit = request.getResultLimit();
        if (limit > 0 && limit < N)
            N = limit;

        for (int i = 0; i < N; i++) {
            CollisionElt elt = get(i);
            elt.exportToJson(builder, th);
        }
    }

}
