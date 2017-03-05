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

import java.util.Arrays;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

public class BytesHandler extends TypeHandlerBase {
    public final static BytesHandler instance = new BytesHandler("byte", false);

    protected BytesHandler(String type, boolean known) {
        super(type, known);
    }

    @Override
    Object[] _bytesToObjects(byte[] bytes) {
        return static_bytesToObjects(bytes);
    }

    @Override
    protected Object[] _docValuesToObjects(AtomicFieldData fieldData, int docid) {
        return static_docValuesToObjects(fieldData, docid);
    }

    @Override
    public byte[] toBytes(String s) {
        byte[] b = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * s.length()];
        int len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), b);
        return Arrays.copyOf(b, len);
    }

    @Override
    public String toString(byte[] bytes) {
        return static_toString(bytes);
    }
    
    
    
    static Object[] static_bytesToObjects(byte[] bytes) {
        Object[] ret = new Object[1];
        ret[0] = static_toString(bytes);
        return ret; 
    }
 
    static Object[]  static_docValuesToObjects(AtomicFieldData fieldData, int docid) {
        try {
            SortedBinaryDocValues bdv = fieldData.getBytesValues();
            bdv.setDocument(docid);
            int N = bdv.count();
            Object[] ret = new Object[N];
            for (int i=0; i<N; i++) {
                ret[i] = bdv.valueAt(i).toString();
            }
            return ret;
        } catch (Throwable th) {
            return new Object[] {th.toString()};
        }
    }
    static String static_toString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        final int end = bytes.length;
        for (int i = 0; i < end; i++) {
            if (i>0 && i%4==0) sb.append(' ');
            sb.append(String.format("%02X", bytes[i] & 0xFF));
        }
        sb.append(']');
        return sb.toString();
    }
}
