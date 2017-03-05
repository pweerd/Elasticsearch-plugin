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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class PatternExtracter {
    final _Extracter[] parts;
    
    public PatternExtracter (String expr) {
        List<_Extracter> list = new ArrayList<_Extracter>();
        
        int prev = 0;
        int i;
        int N = expr==null ? 0 : expr.length();
        for (i=0; i<N; i++) {
            if (expr.charAt(i) != '$') continue;
            if (i+1>=N) continue;
            if (expr.charAt(i+1) == '$') {
                if (i>=prev) list.add (new _ConstExtracter (expr, prev, i+1));
                prev = i+2; //skip the $
                ++i;
                continue;
            }
            
            int ch = expr.charAt(i+1) - '0';
            if (ch < 0 || ch > 9) continue;
            list.add (new _GroupExtracter (ch));
            prev = i+2; //skip the number
            ++i;
        }
        if (prev < N) list.add (new _ConstExtracter (expr, prev, N));
        
        N = list.size();
        parts = N==0 ? null : list.toArray(new _Extracter[N]);
    }
    
    public String extract (Matcher m) {
        int N = parts.length;
        if (N==0) return "";
        
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<N; i++)
            parts[i].append(sb,  m);
        return sb.toString();
    }
    
    
    static abstract class _Extracter {
        abstract void append (StringBuilder sb, Matcher m);
    }
    
    static class _ConstExtracter extends _Extracter {
        private final String value;
        _ConstExtracter (String value, int start, int end) {
            this.value = (start >= end) ? "" : value.substring(start, end);
        }
        
        @Override
        void append (StringBuilder sb, Matcher m) {
            sb.append(value);
        }
        
        @Override
        public String toString() {
            return String.format("const[%s]", value);  
        }
    }

    static class _GroupExtracter extends _Extracter {
        private final int group;
        _GroupExtracter (int group) {
            this.group = group;
        }
        
        @Override
        void append (StringBuilder sb, Matcher m) {
            sb.append(m.group(group));
        }
        
        @Override
        public String toString() {
            return String.format("grp[%d]", group);  
        }
    }
}
