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

package nl.bitmanager.elasticsearch.typehandlers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.mapper.MappedFieldType;

import nl.bitmanager.elasticsearch.support.Utils;

public abstract class TypeHandler {
    public final String typeName;
    public final boolean knowType;
    public final NumericType numericType;
    public static final Object[] NO_DOCVALUES = new Object[0];

    public abstract Object[] docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException;
    public abstract XContentBuilder export(XContentBuilder builder, byte[] bytes) throws IOException;

    public XContentBuilder export(XContentBuilder builder, String field, byte[] bytes) throws IOException {
        return export (builder.field(field), bytes);
    }

    public abstract byte[] toBytes(String s);

    public abstract String toString(byte[] b);

    protected TypeHandler(String type, NumericType numType, boolean known) {
        this.typeName = type;
        this.knowType = known;
        this.numericType = numType;
    }

    public static TypeHandler create(MappedFieldType mft, String field) {
        return create(mft == null ? field : mft.typeName());
    }

    public static TypeHandler create(String type) {
        TypeHandler ret = _types.get(type);
        if (ret == null)  {
            System.out.printf("Unknown type [%s]. Using ByteHandler.\n", type);
            Utils.printStackTrace("Unknown handler");
            ret = new BytesHandler(type, false);
        }
        return ret;
    }

    public static byte[] toByteArray(BytesRef br) {
        byte[] bytes = br.bytes;
        return (br.offset==0 && br.length == bytes.length) ? br.bytes : Arrays.copyOfRange(bytes, br.offset, br.offset+br.length);
    }

    static HashMap<String, TypeHandler> _types;
    static {
        HashMap<String, TypeHandler> map = new HashMap<String, TypeHandler>(25);
        map.put("text", new StringHandler("text"));
        map.put("keyword", new StringHandler("keyword"));
        map.put("text_with_docvalues", new StringHandler("text_with_docvalues"));
        map.put("analyzed_keyword", new StringHandler("analyzed_keyword"));
        map.put("string", new StringHandler("string"));
        map.put("_all", new StringHandler("_all"));
        map.put("_parent", new StringHandler("_parent"));
        map.put("parent", new StringHandler("parent"));
        map.put("join", new StringHandler("join"));
        map.put("_routing", new StringHandler("_routing"));
        map.put("_type", new StringHandler("_type"));
        map.put("_uid", new StringHandler("_uid"));
        map.put("_field_names", new StringHandler("_field_names"));

        map.put("bool", new BoolHandler("bool"));
        map.put("boolean", new BoolHandler("boolean"));

        map.put("long", new Int64Handler("long"));
        map.put("_seq_no", new Int64Handler("_seq_no"));
        map.put("_version", new Int64Handler("_version"));

        map.put("integer", new Int32Handler("integer"));
        map.put("short", new Int32Handler("short"));
        map.put("byte", new Int32Handler("byte"));

        map.put("float", new Float32Handler("float"));
        map.put("double", new Float64Handler("double"));

        map.put("geo_point", new GeoPointHandler("geo_point"));

        map.put("date", new DateHandler("date"));

        map.put("_id", new IDHandler("_id"));
        map.put("_source", new StringHandler("_source"));
        map.put("_primary_term", new Int64Handler("_primary_term"));


        _types = map;
    }



}
