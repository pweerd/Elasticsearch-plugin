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

public class PatternExtracterTest {

    @Test
    public void testEmpty() {
        PatternExtracter pe;
        pe = new PatternExtracter(null);
        assertEquals (null, (Object)pe.parts);
        pe = new PatternExtracter("");
        assertEquals (null, (Object)pe.parts);
    }

    @Test
    public void testOthers() {
        check ("abc", "const[abc]");
        check ("ab$$c", "const[ab$]", "const[c]");
        check ("a$$", "const[a$]");
        check ("ab$$$1", "const[ab$]", "grp[1]");
        check ("ab$$$a", "const[ab$]", "const[$a]");
        check ("ab$$$1x", "const[ab$]", "grp[1]", "const[x]");
        check ("ab$$$1$0x", "const[ab$]", "grp[1]", "grp[0]", "const[x]");
    }

    private void check (String expr, String ... expected) {
        PatternExtracter pe = new PatternExtracter(expr);
        
        int N = pe.parts.length;
        System.out.printf ("%s --> %d parts\n", expr, N);
        for (int i=0; i<N; i++) {
            System.out.printf ("[%02d]: %s\n", i, pe.parts[i]);
        }
        
        for (int i=0; i<N; i++) {
            assertTrue (i<expected.length);
            assertEquals (expected[i], pe.parts[i].toString());
        }
        assertEquals (N, expected.length);
        
    }
}
