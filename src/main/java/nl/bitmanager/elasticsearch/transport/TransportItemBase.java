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

package nl.bitmanager.elasticsearch.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import nl.bitmanager.elasticsearch.support.Utils;

/**
 * Base class for shard requests supply some helper methods to make it easier to
 * stream content into a streamable
 * 
 * @author pweerd
 * 
 */
public abstract class TransportItemBase implements Streamable, ToXContent {

    /** Help to identify where this transportitem came from */
    public final String creationId;

    protected TransportItemBase() {
        creationId = String.format("ctr[%d]", System.identityHashCode(this));
    }

    protected TransportItemBase(String what) {
        creationId = String.format("%s[%d]", what, System.identityHashCode(this));
    }

    @Override
    public String toString() {
        return String.format("%s, creationid=%s", Utils.getTrimmedClass(this), creationId);
    }

    protected abstract void consolidateResponse(TransportItemBase other);

    public static String readStr(StreamInput in) throws IOException {
        return nullIfEmpty(in.readString());
    }

    public static void writeStr(StreamOutput out, String x) throws IOException {
        out.writeString(emptyIfNull(x));
    }

    protected static byte[] readByteArray(StreamInput in) throws IOException {
        return (in.readBoolean()) ? in.readByteArray() : null;
    }

    protected static void writeByteArray(StreamOutput out, byte[] x) throws IOException {
        if (x == null)
            out.writeBoolean(false);
        else {
            out.writeBoolean(true);
            out.writeByteArray(x);
        }
    }

    protected static String emptyIfNull(String x) {
        return x == null ? "" : x;
    }

    protected static String nullIfEmpty(String x) {
        return (x == null || x.length() == 0) ? null : x;
    }

    protected static Throwable readThrowable(StreamInput in) throws IOException {
        return (Throwable) readObject(in);
    }

    protected static void writeThrowable(StreamOutput out, Throwable x) throws IOException {
        writeObject(out, x);
    }

    protected static Object readObject(StreamInput in) throws IOException {
        int x = in.readVInt();
        if (x == 0)
            return null;

        byte[] bytes = new byte[x];
        in.readBytes(bytes, 0, x);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream strm = new ObjectInputStream(byteStream);
        try {
            return strm.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    protected static void writeObject(StreamOutput out, Object x) throws IOException {
        if (x == null) {
            out.writeVInt(0);
            return;
        }

        // Stream the object into a temp. byte array
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream strm = new ObjectOutputStream(byteStream);
        strm.writeObject(x);
        strm.flush();

        // And write that to the output stream
        out.writeVInt(byteStream.size());
        byteStream.writeTo(out);
    }

    protected static void writeSet(StreamOutput out, Set<String> set) throws IOException {
        int N = set == null ? 0 : set.size();
        out.writeVInt(N);
        if (set == null)
            return;

        for (String s : set) {
            writeStr(out, s);
        }
    }

    protected static Set<String> readSet(StreamInput in) throws IOException {
        Set<String> set = new TreeSet<String>();
        int N = in.readVInt();
        for (int i = 0; i < N; i++) {
            set.add(in.readString());
        }
        return set;
    }

    protected static void writeMap(StreamOutput out, Map<String, String> map) throws IOException {
        if (map == null)
            out.writeVInt(0);
        else {
            out.writeVInt(map.size());
            for (Entry<String, String> kvp : map.entrySet()) {
                writeStr(out, kvp.getKey());
                writeStr(out, kvp.getValue());
            }
        }
    }

    protected static Map<String, String> readMap(StreamInput in) throws IOException {
        Map<String, String> map = new HashMap<String, String>();
        int N = in.readVInt();
        for (int i = 0; i < N; i++) {
            String k = in.readString();
            String v = in.readString();
            map.put(k, v);
        }
        return map;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new RuntimeException(Utils.getTrimmedClass(this) + " does not override toXContent().");
    }

}
