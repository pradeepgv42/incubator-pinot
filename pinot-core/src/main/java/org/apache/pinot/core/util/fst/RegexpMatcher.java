/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.util.fst;

import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RegexpMatcher {
    public static final Logger LOGGER =
            LoggerFactory.getLogger(FSTBuilder.class);

    private final String _regexQuery;
    private final FST<Long> _fst;
    private final Automaton _automaton;

    public RegexpMatcher(String regexQuery, FST<Long> fst) {
        _regexQuery = regexQuery;
        _fst = fst;
        _automaton = (new RegExp(_regexQuery)).toAutomaton();

    }

    public static final class Path<T> {
        public final int state;
        public final FST.Arc<T> fstNode;
        public final T output;
        public final IntsRefBuilder input;

        public Path(int state, FST.Arc<T> fstNode, T output, IntsRefBuilder input) {
            this.state = state;
            this.fstNode = fstNode;
            this.output = output;
            this.input = input;
        }
    }

    // Matches "input" string with _regexQuery Automaton.
    public boolean match(String input) {
        CharacterRunAutomaton characterRunAutomaton =
                new CharacterRunAutomaton(_automaton);
        return characterRunAutomaton.run(input);
    }

    public static List<Long> regexMatch(String regexQuery, FST<Long> fst) throws IOException {
        RegexpMatcher matcher = new RegexpMatcher(regexQuery, fst);
        return matcher.regexMatchOnFST();
    }

    public List<Long> regexMatchOnFST() throws IOException {
        final List<Path<Long>> queue = new ArrayList<>();
        final List<Path<Long>> endNodes = new ArrayList<>();
        if (_automaton.getNumStates() == 0) {
            return Collections.emptyList();
        }

        queue.add(new Path<>(0, _fst
                .getFirstArc(new FST.Arc<Long>()), _fst.outputs.getNoOutput(),
                new IntsRefBuilder()));

        final FST.Arc<Long> scratchArc = new FST.Arc<>();
        final FST.BytesReader fstReader = _fst.getBytesReader();

        Transition t = new Transition();
        while (queue.size() != 0) {
            final Path<Long> path = queue.remove(queue.size() - 1);
            if (_automaton.isAccept(path.state)) {
                if (path.fstNode.isFinal()) {
                    endNodes.add(path);
                }
            }

            IntsRefBuilder currentInput = path.input;
            int count = _automaton.initTransition(path.state, t);
            for (int i = 0; i < count; i++) {
                _automaton.getNextTransition(t);
                final int min = t.min;
                final int max = t.max;
                if (min == max) {
                    final FST.Arc<Long> nextArc = _fst.findTargetArc(t.min,
                            path.fstNode, scratchArc, fstReader);
                    if (nextArc != null) {
                        final IntsRefBuilder newInput = new IntsRefBuilder();
                        newInput.copyInts(currentInput.get());
                        newInput.append(t.min);
                        queue.add(new Path<Long>(t.dest, new FST.Arc<Long>()
                                .copyFrom(nextArc), _fst.outputs.add(path.output, nextArc.output), newInput));
                    }
                } else {
                    FST.Arc<Long> nextArc = Util.readCeilArc(min, _fst, path.fstNode,
                            scratchArc, fstReader);
                    while (nextArc != null && nextArc.label <= max) {
                        final IntsRefBuilder newInput = new IntsRefBuilder();
                        newInput.copyInts(currentInput.get());
                        newInput.append(nextArc.label);
                        queue.add(new Path<>(t.dest, new FST.Arc<Long>()
                                .copyFrom(nextArc), _fst.outputs
                                .add(path.output, nextArc.output), newInput));
                        nextArc = nextArc.isLast() ? null : _fst.readNextRealArc(nextArc,
                                fstReader);
                    }
                }
            }
        }
        ArrayList<Long> matchedIds = new ArrayList<>();
        for (Path<Long> path : endNodes) {
            matchedIds.add(path.output);
        }
        return matchedIds;
    }
}
