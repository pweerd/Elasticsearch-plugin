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

public class RegexReplacers {
    private ArrayList<RegexReplace> replacers;

    public RegexReplacers(String expr) {
        if (expr == null || expr.length() == 0)
            return;

        String[] arr = expr.split("\\|\\|");
        System.out.println("Found " + arr.length + " replacers");
        replacers = new ArrayList<RegexReplace>(arr.length);
        for (int i = 0; i < arr.length; i++) {
            System.out.println("-- '" + arr[i] + "'");
            replacers.add(new RegexReplace(arr[i]));
        }
    }

    public boolean hasReplacements() {
        return replacers != null;
    }

    public String replace(String txt) {
        if (txt == null || txt.length() == 0 || replacers == null)
            return null;
        int replaced = 0;
        for (int i = 0; i < replacers.size(); i++) {
            String tmp = replacers.get(i).replace(txt);
            if (tmp == null)
                continue;
            txt = tmp;
            replaced++;
        }
        return replaced == 0 ? null : txt;
    }
}
