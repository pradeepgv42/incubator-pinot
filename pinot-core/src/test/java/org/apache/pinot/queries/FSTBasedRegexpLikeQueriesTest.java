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
package org.apache.pinot.queries;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class FSTBasedRegexpLikeQueriesTest extends BaseQueriesTest {
    private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueriesTest");
    private static final String TABLE_NAME = "MyTable";
    private static final String SEGMENT_NAME = "testSegment";
    private static final String DOMAIN_NAMES_COL = "DOMAIN_NAMES";
    private static final String URL_COL = "URL_COL";
    private static final String INT_COL_NAME = "INT_COL";
    private static final Integer INT_BASE_VALUE = 1000;
    private static final Integer NUM_ROWS = 1024;

    private final List<GenericRow> _rows = new ArrayList<>();

    private IndexSegment _indexSegment;
    private List<IndexSegment> _indexSegments;

    @Override
    protected String getFilter() {
        return "";
    }

    @Override
    protected IndexSegment getIndexSegment() {
        return _indexSegment;
    }

    @Override
    protected List<IndexSegment> getIndexSegments() {
        return _indexSegments;
    }

    @BeforeClass
    public void setUp()
            throws Exception {
        FileUtils.deleteQuietly(INDEX_DIR);

        buildSegment();
        IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
        Set<String> fstIndexCols = new HashSet<>();
        fstIndexCols.add(DOMAIN_NAMES_COL);
        indexLoadingConfig.setFSTIndexColumns(fstIndexCols);

        Set<String> invertedIndexCols = new HashSet<>();
        invertedIndexCols.add(DOMAIN_NAMES_COL);
        indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
        ImmutableSegment immutableSegment =
                ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
        _indexSegment = immutableSegment;
        _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
    }

    @AfterClass
    public void tearDown() {
        _indexSegment.destroy();
        FileUtils.deleteQuietly(INDEX_DIR);
    }

    private List<String> getURLSufficies() {
        return Arrays.asList(
                "/a", "/b", "/c", "/d"
        );
    }

    private List<String> getDomainNames() {
        return Arrays.asList(
                "www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd",
                "www.sd.domain1.com", "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd",
                "www.domain2.com", "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd",
                "www.sd.domain2.com", "www.sd.domain2.co.ab", "www.sd.domain2.co.bc", "www.sd.domain2.co.cd"
        );
    }

    private List<GenericRow> createTestData(int numRows) throws Exception {
        List<GenericRow> rows = new ArrayList<>();
        List<String> domainNames = getDomainNames();
        List<String> urlSufficies = getURLSufficies();
        for (int i = 0; i < numRows; i++) {
            String domain = domainNames.get(i % domainNames.size());
            String url = domain + urlSufficies.get(i % urlSufficies.size());

            GenericRow row = new GenericRow();
            row.putField(INT_COL_NAME, INT_BASE_VALUE + i);
            row.putField(DOMAIN_NAMES_COL, domain);
            row.putField(URL_COL, url);
            rows.add(row);
        }
        return rows;
    }

    private void buildSegment()
            throws Exception {
        List<GenericRow> rows = createTestData(NUM_ROWS);
        List<FieldConfig> fieldConfigs = new ArrayList<>();
        fieldConfigs.add(new FieldConfig(
                DOMAIN_NAMES_COL,
                FieldConfig.EncodingType.DICTIONARY,
                FieldConfig.IndexType.FST, null));
        fieldConfigs.add(new FieldConfig(
                URL_COL,
                FieldConfig.EncodingType.DICTIONARY,
                FieldConfig.IndexType.FST, null));

        TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
                .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL))
                .setFieldConfigList(fieldConfigs).build();
        Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
                .addSingleValueDimension(DOMAIN_NAMES_COL, FieldSpec.DataType.STRING)
                .addSingleValueDimension(URL_COL, FieldSpec.DataType.STRING)
                .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
        config.setOutDir(INDEX_DIR.getPath());
        config.setTableName(TABLE_NAME);
        config.setSegmentName(SEGMENT_NAME);

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
            driver.init(config, recordReader);
            driver.build();
        }
    }

    private void testSelectionResults(String query, int expectedResultSize)
            throws Exception {
        Operator<IntermediateResultsBlock> operator = getOperatorForPqlQuery(query);
        IntermediateResultsBlock operatorResult = operator.nextBlock();
        List<Object[]> resultset = (List<Object[]>) operatorResult.getSelectionResult();
        Assert.assertNotNull(resultset);
        Assert.assertEquals(resultset.size(), expectedResultSize);
    }

    private AggregationGroupByResult getGroupByResults(String query)
            throws Exception {
        AggregationGroupByOperator operator = getOperatorForPqlQuery(query);
        IntermediateResultsBlock resultsBlock = operator.nextBlock();
        return resultsBlock.getAggregationGroupByResult();
    }

    private void matchGroupResult(AggregationGroupByResult result, String key, long count) {
        Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = result.getGroupKeyIterator();
        while (groupKeyIterator.hasNext()) {
            GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
            Assert.assertEquals(((Number)result.getResultForKey(groupKey, 0)).longValue(), count);
        }
    }

    @Test
    public void testFSTBasedRegexpLike() throws Exception {
        // Select queries on col with FST + inverted index.
        String query =
                "SELECT INT_COL, DOMAIN_NAMES FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') LIMIT 50000";
        testSelectionResults(query, 256);

        query = "SELECT INT_COL, DOMAIN_NAMES FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.sd.domain1.*') LIMIT 50000";
        testSelectionResults(query, 256);

        query = "SELECT INT_COL, DOMAIN_NAMES FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain1.*') LIMIT 50000";
        testSelectionResults(query, 512);

        query = "SELECT INT_COL, DOMAIN_NAMES FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain.*') LIMIT 50000";
        testSelectionResults(query, 1024);

        query = "SELECT INT_COL, DOMAIN_NAMES FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*com') LIMIT 50000";
        testSelectionResults(query, 256);

        // Select queries on col with just FST index.
        query =
                "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 50000";
        testSelectionResults(query, 256);

        query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.sd.domain1.*') LIMIT 50000";
        testSelectionResults(query, 256);

        query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain1.*') LIMIT 50000";
        testSelectionResults(query, 512);

        query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 50000";
        testSelectionResults(query, 1024);

        query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 50000";
        testSelectionResults(query, 256);


        query = "SELECT DOMAIN_NAMES, count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') group by DOMAIN_NAMES LIMIT 50000";
        AggregationGroupByResult result = getGroupByResults(query);
        matchGroupResult(result, "www.domain1.com/a", 64);
        matchGroupResult(result, "www.domain1.co.ab/b", 64);
        matchGroupResult(result, "www.domain1.co.bc/c", 64);
        matchGroupResult(result, "www.domain1.co.cd/d", 64);



        query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a') group by DOMAIN_NAMES LIMIT 50000";
        result = getGroupByResults(query);
        matchGroupResult(result, "www.domain1.com/a", 64);
    }
}
