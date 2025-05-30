/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Tests for {@link ProcTimeDeduplicateKeepLastRowFunction}. */
class ProcTimeDeduplicateKeepLastRowFunctionTest extends ProcTimeDeduplicateFunctionTestBase {
    private ProcTimeDeduplicateKeepLastRowFunction createFunctionWithoutStateTtl(
            boolean generateUpdateBefore, boolean generateInsert) {
        return new ProcTimeDeduplicateKeepLastRowFunction(
                inputRowType,
                0,
                generateUpdateBefore,
                generateInsert,
                true,
                generatedEqualiser,
                null);
    }

    private ProcTimeDeduplicateKeepLastRowFunction createFunction(
            boolean generateUpdateBefore, boolean generateInsert) {
        return new ProcTimeDeduplicateKeepLastRowFunction(
                inputRowType,
                minTime.toMillis(),
                generateUpdateBefore,
                generateInsert,
                true,
                generatedEqualiser,
                null);
    }

    private ProcTimeDeduplicateKeepLastRowFunction createFunctionWithFilter(
            boolean generateUpdateBefore) {
        return new ProcTimeDeduplicateKeepLastRowFunction(
                inputRowType,
                minTime.toMillis(),
                generateUpdateBefore,
                true,
                false,
                generatedEqualiser,
                generatedFilterCondition);
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            ProcTimeDeduplicateKeepLastRowFunction func) throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator = new KeyedProcessOperator<>(func);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, rowKeySelector, rowKeySelector.getProducedType());
    }

    @Test
    void testWithoutGenerateUpdateBefore() throws Exception {
        ProcTimeDeduplicateKeepLastRowFunction func = createFunction(false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 13));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // we do not output INSERT if generateUpdateBefore is false for deduplication last row
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 1L, 13));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithoutGenerateUpdateBeforeAndInsert() throws Exception {
        ProcTimeDeduplicateKeepLastRowFunction func = createFunction(false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 13));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // we always produce UPDATE_AFTER if INSERT is not needed.
        expectedOutput.add(updateAfterRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 1L, 13));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithGenerateUpdateBefore() throws Exception {
        ProcTimeDeduplicateKeepLastRowFunction func = createFunction(true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.close();

        // Keep LastRow in deduplicate may send UPDATE_BEFORE
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(updateBeforeRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 11));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithStateTtlDisabled() throws Exception {
        ProcTimeDeduplicateKeepLastRowFunction func = createFunctionWithoutStateTtl(true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.close();

        // Keep LastRow in deduplicate may send UPDATE_BEFORE
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 11));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testWithGenerateUpdateBeforeAndStateTtl() throws Exception {
        ProcTimeDeduplicateKeepLastRowFunction func = createFunction(true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("book", 1L, 13));

        testHarness.setStateTtlProcessingTime(30);
        testHarness.processElement(insertRecord("book", 1L, 17));
        testHarness.processElement(insertRecord("book", 2L, 18));
        testHarness.processElement(insertRecord("book", 1L, 19));

        // Keep LastRow in deduplicate may send UPDATE_BEFORE
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(updateBeforeRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 1L, 13));
        expectedOutput.add(insertRecord("book", 2L, 11));
        // because (2L,11), (1L,13) retired, so no UPDATE_BEFORE message send to downstream
        expectedOutput.add(insertRecord("book", 1L, 17));
        expectedOutput.add(updateBeforeRecord("book", 1L, 17));
        expectedOutput.add(updateAfterRecord("book", 1L, 19));
        expectedOutput.add(insertRecord("book", 2L, 18));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWithFilterCondition(boolean updateBefore) throws Exception {
        ProcTimeDeduplicateKeepLastRowFunction func = createFunctionWithFilter(updateBefore);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(updateAfterRecord("book", 1L, 12));
        testHarness.processElement(updateAfterRecord("book", 1L, 15));
        testHarness.processElement(updateAfterRecord("book", 1L, 8));
        testHarness.processElement(updateAfterRecord("book", 1L, 9));
        testHarness.processElement(updateAfterRecord("book", 1L, 13));
        testHarness.processElement(deleteRecord("book", 1L, null));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        if (updateBefore) {
            expectedOutput.add(updateBeforeRecord("book", 1L, 12));
        }
        expectedOutput.add(updateAfterRecord("book", 1L, 15));
        expectedOutput.add(deleteRecord("book", 1L, 15));
        expectedOutput.add(insertRecord("book", 1L, 13));
        expectedOutput.add(deleteRecord("book", 1L, 13));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }
}
