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

import static org.junit.Assert.*;

import org.junit.Test;

public class RegexReplacerTest {

    @Test
    public void test() {
        check ("a/b", "a", "b");
        check ("a/", "a", "");
        check ("a///", "a/", "");
        check ("a///b//", "a/", "b/");
    }

    private void check (String expr, String exprVal, String replVal) {
        RegexReplace pe = new RegexReplace(expr);
        assertEquals (exprVal, pe.expr);
        assertEquals (replVal, pe.repl);
    }

}
