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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class CollisionElt {
    public final TermElt term, collidingTerm;

    public CollisionElt(TermElt term, TermElt collidingTerm) {
        this.term = term;
        this.collidingTerm = collidingTerm;
    }

    public CollisionElt(StreamInput in) throws IOException {
        this.term = new TermElt(in);
        if (in.readVInt() == 0)
            this.collidingTerm = null;
        else
            this.collidingTerm = new TermElt(in);
    }

    public void saveToStream(StreamOutput out) throws IOException {
        term.saveToStream(out);
        if (collidingTerm == null) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(0);
        collidingTerm.saveToStream(out);
    }

    public void exportToJson(XContentBuilder builder, TypeHandler th) throws IOException {
        builder.startObject();
        builder.field("term");
        term.exportToJson(builder, th);
        if (collidingTerm != null) {
            builder.field("coll");
            collidingTerm.exportToJson(builder, th);
        }
        builder.endObject();
    }
}
