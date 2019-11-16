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

public class IntRange {
    public final int low;
    public final int high;

    public IntRange (String x) {
        this (x, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
    public IntRange (String x, int defLo) {
        this (x, defLo, Integer.MAX_VALUE);
    }
    public IntRange (String x, int defLo, int defHi) {
        int low = defLo;
        int high = defHi;

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
            low = Integer.parseInt(left);
        if (right.length() > 0)
            high = left==right ? low+1 : Integer.parseInt(right);

        this.low = low;
        this.high = high;
    }

    @Override
    public String toString() {
        return String.format("%d..%d",  low, high);
    }

    public boolean isInRange (int x) {
        return x >= low && x < high;
    }

}
