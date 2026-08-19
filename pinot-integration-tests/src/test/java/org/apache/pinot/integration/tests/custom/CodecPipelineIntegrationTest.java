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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// End-to-end proof that every codecSpec uses the V7 format and is queryable through both engines.
@Test(suiteName = "CustomClusterIntegrationTest")
public class CodecPipelineIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "CodecPipelineIntegrationTest";
  protected static final String INT_COL = "intLz4";
  protected static final String LONG_COL = "longDeltaLz4";
  protected static final String TIME_COL = "ts";
  private static final int NUM_DOCS = 600;
  private static final int DOCS_PER_CHUNK = 128;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMetric(INT_COL, FieldSpec.DataType.INT)
        .addMetric(LONG_COL, FieldSpec.DataType.LONG)
        .addDateTimeField(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("codecRecord", null, null, false);
    avroSchema.setFields(List.of(
        new Field(INT_COL, org.apache.avro.Schema.create(Type.INT), null, null),
        new Field(LONG_COL, org.apache.avro.Schema.create(Type.LONG), null, null),
        new Field(TIME_COL, org.apache.avro.Schema.create(Type.LONG), null, null)));
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COL, i);
        record.put(LONG_COL, (long) i * 1_000_000_000L);
        record.put(TIME_COL, (long) i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COL;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setFieldConfigList(getFieldConfigs())
        .build();
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return List.of(INT_COL, LONG_COL);
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return List.of(rawFieldConfigWithCodecSpec(INT_COL, "LZ4"),
        rawFieldConfigWithCodecSpec(LONG_COL, "DELTA,LZ4"));
  }

  private static FieldConfig rawFieldConfigWithCodecSpec(String column, String codecSpec) {
    ObjectNode forward = JsonUtils.newObjectNode();
    forward.put("codecSpec", codecSpec);
    forward.put("targetDocsPerChunk", DOCS_PER_CHUNK);
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("forward", forward);
    return new FieldConfig.Builder(column)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();
    JsonNode aggregate = postQuery("SELECT SUM(" + INT_COL + "), SUM(" + LONG_COL + ") FROM " + tableName);
    JsonNode aggregateRow = aggregate.get("resultTable").get("rows").get(0);
    assertEquals(aggregateRow.get(0).asLong(), 179_700L);
    assertEquals(aggregateRow.get(1).asLong(), 179_700_000_000_000L);

    JsonNode filter = postQuery("SELECT COUNT(*) FROM " + tableName + " WHERE " + INT_COL + " < 100");
    assertEquals(filter.get("resultTable").get("rows").get(0).get(0).asLong(), 100L);

    int[] ids = {0, 255, 256, 599};
    JsonNode rows = postQuery("SELECT " + TIME_COL + ", " + INT_COL + ", " + LONG_COL + " FROM " + tableName
        + " WHERE " + TIME_COL + " IN (0, 255, 256, 599) ORDER BY " + TIME_COL).get("resultTable").get("rows");
    assertEquals(rows.size(), ids.length);
    for (int i = 0; i < ids.length; i++) {
      int id = ids[i];
      assertEquals(rows.get(i).get(0).asInt(), id);
      assertEquals(rows.get(i).get(1).asInt(), id);
      assertEquals(rows.get(i).get(2).asLong(), (long) id * 1_000_000_000L);
    }
  }

  @Test
  public void testGeneratedSegmentsUseV7()
      throws Exception {
    File[] segmentDirs = _segmentDir.listFiles(File::isDirectory);
    assertNotNull(segmentDirs);
    assertTrue(segmentDirs.length > 0);
    FieldIndexConfigs rawForwardConfig = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.forward(), new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW).build())
        .build();
    for (File segmentDir : segmentDirs) {
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentDirectory.Reader segmentReader = directory.createReader()) {
        assertV7(directory, segmentReader, rawForwardConfig, INT_COL, "LZ4");
        assertV7(directory, segmentReader, rawForwardConfig, LONG_COL, "DELTA,LZ4");
      }
    }
  }

  private static void assertV7(SegmentDirectory directory, SegmentDirectory.Reader segmentReader,
      FieldIndexConfigs rawForwardConfig, String column, String codecSpec)
      throws Exception {
    ColumnMetadata metadata = directory.getSegmentMetadata().getColumnMetadataFor(column);
    assertNotNull(metadata);
    assertEquals(metadata.getForwardIndexEncoding(), FieldConfig.EncodingType.RAW);
    assertFalse(metadata.hasDictionary());
    try (ForwardIndexReader<?> reader = StandardIndexes.forward().getReaderFactory()
        .createIndexReader(segmentReader, rawForwardConfig, metadata)) {
      assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
          column + " was routed to " + reader.getClass().getSimpleName());
      assertEquals(reader.getCodecSpec(), codecSpec);
      assertNull(reader.getCompressionType());
    }
  }
}
