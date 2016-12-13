/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.aliyun.tablestore.hive;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.spark.aliyun.tablestore.hadoop.Credential;
import org.apache.spark.aliyun.tablestore.hadoop.Endpoint;
import org.apache.spark.aliyun.tablestore.hadoop.PrimaryKeyWritable;
import org.apache.spark.aliyun.tablestore.hadoop.TableStoreRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hive.serde.serdeConstants;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import org.apache.spark.aliyun.tablestore.hadoop.RowWritable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreInputFormat implements InputFormat<PrimaryKeyWritable, RowWritable> {
    private static Logger logger = LoggerFactory.getLogger(TableStoreInputFormat.class);

    @Override
    public InputSplit[] getSplits(
        JobConf job,
        int numSplits)
        throws IOException
    {
        Configuration dest = translateConfig(job);
        SyncClientInterface ots = null;
        String columns = job.get(TableStoreConsts.COLUMNS_MAPPING);
        if (columns == null) {
            columns = job.get(serdeConstants.LIST_COLUMNS);
        }
        logger.debug("columns to get: {}", columns);
        List<org.apache.hadoop.mapreduce.InputSplit> splits;
        try {
            ots = org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat
                .newOtsClient(dest);
            org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat
                .addCriteria(
                    dest,
                    fetchCriteria(
                        fetchTableMeta(
                            ots,
                            job.get(TableStoreConsts.TABLE_NAME)),
                        columns));
            splits = org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat
                .getSplits(dest, ots);
        } finally {
            if (ots != null) {
                ots.shutdown();
                ots = null;
            }
        }
        InputSplit[] res = new InputSplit[splits.size()];
        int i = 0;
        for(org.apache.hadoop.mapreduce.InputSplit split: splits) {
            res[i] = new TableStoreInputSplit(
                (org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputSplit) split);
            ++i;
        }
        return res;
    }

    private static Configuration translateConfig(Configuration from) {
        Configuration to = new Configuration();
        {
            Credential cred =
                new Credential(
                    from.get(TableStoreConsts.ACCESS_KEY_ID),
                    from.get(TableStoreConsts.ACCESS_KEY_SECRET),
                    from.get(TableStoreConsts.SECURITY_TOKEN));
            org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat
                .setCredential(to, cred);
        }
        {
            String endpoint = from.get(TableStoreConsts.ENDPOINT);
            String instance = from.get(TableStoreConsts.INSTANCE);
            Endpoint ep;
            if (instance == null) {
                ep = new Endpoint(
                    endpoint);
            } else {
                ep = new Endpoint(
                    endpoint, instance);
            }
            org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat
                .setEndpoint(to, ep);
        }
        return to;
    }

    private static RangeRowQueryCriteria fetchCriteria(TableMeta meta, String strColumns)
    {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(meta.getTableName());
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema schema: meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        res.addColumnsToGet(strColumns.split(","));
        return res;
    }

    private static TableMeta fetchTableMeta(
        SyncClientInterface ots,
        String table)
    {
        DescribeTableResponse resp = ots.describeTable(
            new DescribeTableRequest(table));
        return resp.getTableMeta();
    }

    @Override
    public RecordReader<PrimaryKeyWritable, RowWritable> getRecordReader(
        InputSplit split,
        JobConf job,
        Reporter reporter)
        throws IOException
    {
        Preconditions.checkNotNull(split, "split must be nonnull");
        Preconditions.checkNotNull(job, "job must be nonnull");
        Preconditions.checkArgument(
            split instanceof TableStoreInputSplit,
            "split must be one of " + TableStoreInputSplit.class.getName());
        TableStoreInputSplit tsSplit = (TableStoreInputSplit) split;

        Configuration conf = translateConfig(job);
        final TableStoreRecordReader rdr =
            new TableStoreRecordReader();
        rdr.initialize(tsSplit.getDelegated(), conf);
        return new RecordReader<PrimaryKeyWritable, RowWritable>() {
            @Override
            public boolean next(PrimaryKeyWritable key, RowWritable value) throws IOException {
                boolean next = rdr.nextKeyValue();
                if (next) {
                    key.setPrimaryKey(rdr.getCurrentKey().getPrimaryKey());
                    value.setRow(rdr.getCurrentValue().getRow());
                }
                return next;
            }

            @Override
            public PrimaryKeyWritable createKey() {
                return new PrimaryKeyWritable();
            }

            @Override
            public RowWritable createValue() {
                return new RowWritable();
            }

            @Override
            public long getPos() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                rdr.close();
            }

            @Override
            public float getProgress() throws IOException {
                return rdr.getProgress();
            }
        };
    }
}
