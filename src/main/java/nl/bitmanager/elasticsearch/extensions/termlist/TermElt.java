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
import java.util.Arrays;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class TermElt {
    public final byte[] term;
    public int count;

    public TermElt(byte[] term) {
        this.term = term;
        count = 0;
    }

    public TermElt(byte[] term, int cnt) {
        this.term = term;
        count = cnt;
    }

    public TermElt(String s) {
        byte[] b = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * s.length()];
        int len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), b);
        term = Arrays.copyOf(b, len);
    }

    public static byte[] stringToUtf8(String s) {
        byte[] b = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * s.length()];
        int len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), b);
        return Arrays.copyOf(b, len);
    }

    public String termAsString() {
        final char[] ref = new char[term.length];
        final int len = UnicodeUtil.UTF8toUTF16(term, 0, term.length, ref);
        return new String(ref, 0, len);
    }

    public TermElt(StreamInput in) throws IOException {
        this.count = in.readVInt();
        int N = in.readVInt();
        this.term = new byte[N];
        in.read(this.term, 0, N);
    }

    public void saveToStream(StreamOutput out) throws IOException {
        out.writeVInt(count);
        out.writeVInt(term.length);
        out.write(term, 0, term.length);
    }

    public void exportToText(StringBuilder bldr) {
        bldr.append(String.format("% 6d", count));
        bldr.append(';');
        bldr.append(termAsString()); // PW moet generieker!
        bldr.append("\r\n");
    }

    public void exportToJson(XContentBuilder builder, TypeHandler th) throws IOException {
        builder.startObject();
        th.export(builder, "t", term);
        builder.field("c", count);
        builder.endObject();
    }

    public void addCount(int cnt) {
        count += cnt;
    }

    // PW moet misschien toch nog...
    // @Override
    // public int hashCode() {
    // return term.hashCode();
    // }
    //
    // @Override
    // public boolean equals(Object other) {
    // if (!(other instanceof TermElt))
    // return false;
    // return term.equals(((TermElt) other).term);
    // }

    @Override
    public String toString() {
        return String.format("Term(c=%d: %s)", count, termAsString()); // PW
    }
}
