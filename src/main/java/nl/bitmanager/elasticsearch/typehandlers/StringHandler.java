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

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

public class StringHandler extends SafeTypeHandler {

    protected StringHandler(String type) {
        super(type, null, true);
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
        byte[] b = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * s.length()];
        int len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), b);
        return Arrays.copyOf(b, len);
    }

    protected static String _toString(byte[] bytes) {
        final char[] ref = new char[bytes.length];
        final int len = UnicodeUtil.UTF8toUTF16(bytes, 0, bytes.length, ref);
        return new String(ref, 0, len);
    }
}
