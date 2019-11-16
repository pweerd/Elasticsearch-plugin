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

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class BytesRange {
    private final static byte[] MIN_VALUE;
    private final static byte[] MAX_VALUE;
    private final String range;

    public final byte[] low;
    public final byte[] high;
    private final boolean includingRight;

    public BytesRange (String x, TypeHandler th) {
        this (x, th, MIN_VALUE, MAX_VALUE);
    }
    public BytesRange (String x, TypeHandler th, byte[] defLo) {
        this (x, th, defLo, MAX_VALUE);
    }
    public BytesRange (String x, TypeHandler th, byte[] defLo, byte[] defHi) {
        byte[] low = defLo;
        byte[] high = defHi;

        String left = "";
        String right = "";
        if (x != null && x.length() > 0) {
            int ix = x.indexOf("..");
            if (ix < 0) {
                left = x; right=x;
                } else {
                    left = x.substring(0,  ix);
                    right = x.substring(ix+2);
                }
        }
        left = left.trim();
        right = right.trim();
        if (left.length() > 0)
            low = th.toBytes(left);
        if (right.length() > 0)
            high = th.toBytes(right);

        this.low = low;
        this.high = high;
        this.range = x;
        this.includingRight = left.length()>0 && left.equals(right);
    }

    @Override
    public String toString() {
        return range;
    }

    public boolean isInRange (byte[] x) {
        if (BytesHelper.bytesComparer.compare(low, x) > 0) return false;
        int rc = BytesHelper.bytesComparer.compare(x, high);
        return includingRight ? (rc <= 0) : (rc<0);
    }

    static {
        MIN_VALUE = new byte[0];
        byte[] hv = new byte[256];
        for (int i=0; i<hv.length; i++) hv[i] = (byte)0xFF;
        MAX_VALUE = hv;
    }
}
