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

package nl.bitmanager.elasticsearch.support;

import java.util.Comparator;

public class BytesHelper {
    public static Comparator<byte[]> bytesComparer = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] arg0, byte[] arg1) {
            // TODO: Once we are on Java 9 replace this by
            // java.util.Arrays#compareUnsigned()
            // which is implemented by a Hotspot intrinsic! Also consider
            // building a
            // Multi-Release-JAR!
            final int N = Math.min(arg0.length, arg1.length);
            for (int i = 0; i < N; i++) {
                int diff = (arg0[i] & 0xFF) - (arg1[i] & 0xFF);
                if (diff != 0)
                    return diff;
            }

            // One is a prefix of the other, or, they are equal:
            return arg0.length - arg1.length;
        }
    };

    public static Comparator<byte[]> bytesComparerRev = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] arg1, byte[] arg0) {
            // TODO: Once we are on Java 9 replace this by
            // java.util.Arrays#compareUnsigned()
            // which is implemented by a Hotspot intrinsic! Also consider
            // building a
            // Multi-Release-JAR!
            final int N = Math.min(arg0.length, arg1.length);
            for (int i = 0; i < N; i++) {
                int diff = (arg0[i] & 0xFF) - (arg1[i] & 0xFF);
                if (diff != 0)
                    return diff;
            }

            // One is a prefix of the other, or, they are equal:
            return arg0.length - arg1.length;
        }
    };

    public static String toString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        final int end = bytes.length;
        for (int i = 0; i < end; i++) {
            if (i>0) sb.append(' ');
            sb.append(Integer.toHexString(bytes[i] & 0xff));
        }
        sb.append(']');
        return sb.toString();
    }


}
