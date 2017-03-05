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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexReplace {
    public final String expr;
    public final String repl;
    private final Pattern pattern;
    private final PatternExtracter extract;

    public RegexReplace(String txt) {
        int sbIdx = 0;
        StringBuilder[] sb = new StringBuilder[2];
        int N = txt == null ? 0 : txt.length();
        for (int i = 0; i < N; i++) {
            if (txt.charAt(i) != '/') {
                if (sb[sbIdx] == null)
                    sb[sbIdx] = new StringBuilder();
                sb[sbIdx].append(txt.charAt(i));
                continue;
            }
            if (i + 1 < N && txt.charAt(i + 1) == '/') {
                if (sb[sbIdx] == null)
                    sb[sbIdx] = new StringBuilder();
                sb[sbIdx].append('/');
                i++; // skip one /
                continue;
            }

            if (++sbIdx >= 2)
                throwError();
        }

        if (sbIdx != 1 || sb[0] == null)
            throwError();
        this.expr = sb[0].toString();
        this.repl = sb[1] == null ? "" : sb[1].toString();
        pattern = Pattern.compile(expr);
        extract = new PatternExtracter(repl);
    }

    private static void throwError() {
        throw new RuntimeException("Replace expression must be in the form <expr>/<repl>");
    }

    public String replace(String txt) {
        if (txt == null || txt.length() == 0)
            return null;
        Matcher m = pattern.matcher(txt);
        if (!m.find())
            return null;

        return m.replaceAll(repl);
    }

    public String extract(String txt) {
        if (txt == null || txt.length() == 0)
            return null;
        Matcher m = pattern.matcher(txt);
        if (!m.find())
            return null;

        return extract.extract(m);
    }

}
