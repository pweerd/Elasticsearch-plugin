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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.Uid;

public class IDHandler extends SafeTypeHandler {

    protected IDHandler(String type) {
        super(type, NumericType.LONG, true);
    }

    @Override
    protected Object[] _bytesToObjects(byte[] bytes) {
        return new Object[] {_toString(bytes)};
    }

    @Override
    protected Object[] _docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException {
        SortedBinaryDocValues dvs = fieldData.getBytesValues();
        if (!dvs.advanceExact(docid)) return NO_DOCVALUES;
        int N = dvs.docValueCount();
        Object[] ret = new Object[N];
        if (N > 0) {
            for (int i = 0; i < N; i++)
                ret[i] = dvs.nextValue().utf8ToString();
        }
        return ret;
    }

    @Override
    public byte[] toBytes(String s) {
        return Uid.encodeId(s).bytes;
    }

    protected static String _toString(byte[] bytes) {
        return Uid.decodeId(bytes);
    }
    protected static String _toString(BytesRef br) {
        byte[] bytes = br.bytes;
        if (br.offset!=0 || br.length != bytes.length) {
            bytes = Arrays.copyOfRange(bytes, br.offset, br.offset+br.length);
        }
        return _toString (bytes);
    }
}
