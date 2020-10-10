package org.sen.hive.events;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.sen.hive.HiveHookContext;
import org.sen.hive.entity.HiveEntity;
import org.sen.hive.lineage.LineageParser;
import org.sen.hive.lineage.bean.ColumnLineage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author
 */
public class CreateHiveProcess extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveProcess.class);

    public CreateHiveProcess(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() throws Exception {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_PROCESS);

        if (!skipProcess()) {
            List<Map<String, Object>> inputs = new ArrayList<>();
            List<Map<String, Object>> outputs = new ArrayList<>();
            HookContext hiveContext = getHiveContext();

            if (hiveContext.getInputs() != null) {
                for (ReadEntity input : hiveContext.getInputs()) {
                    String qualifiedName = getQualifiedName(input);

                    if (qualifiedName == null) {
                        continue;
                    }

                    Map<String, Object> entity = getInputOutputEntity(input);

                    if (entity != null) {
                        inputs.add(entity);
                    }
                }
            }

            if (hiveContext.getOutputs() != null) {
                for (WriteEntity output : hiveContext.getOutputs()) {
                    String qualifiedName = getQualifiedName(output);

                    if (qualifiedName == null) {
                        continue;
                    }

                    Map<String, Object> entity = getInputOutputEntity(output);

                    if (entity != null) {
                        outputs.add(entity);
                    }
                }
            }

            if (!inputs.isEmpty() || !outputs.isEmpty()) {
                ret.setAttribute(ATTRIBUTE_INPUTS, inputs);
                ret.setAttribute(ATTRIBUTE_OUTPUTS, outputs);
                List<Map<String, Object>> columnLineage = processColumnLineageWithParser();
                if (!columnLineage.isEmpty()) {
                    ret.setAttribute(ATTRIBUTE_COLUMN_LINEAGE, columnLineage);
                }
            }
        }

        return ret;
    }

    private List<Map<String, Object>> processColumnLineageWithParser() {
        List<Map<String, Object>> ret = new ArrayList<>();
        try {
            LineageParser parser = new LineageParser(context);
            parser.getLineageInfo(context.getQueryStr(false));

            List<ColumnLineage> colLines = parser.getColumnLineages();
            if (colLines != null) {
                for (ColumnLineage col : colLines) {
                    String outputColName = col.getToColumnQualifiedName();

                    List<Map<String, Object>> inputColumns = new ArrayList<>();
                    for (String fromColumnName : col.getFromNameSet()) {
                        Map<String, Object> inputColumn = new HashMap<>();
                        inputColumn.put(ATTRIBUTE_QUALIFIED_NAME, fromColumnName);
                        inputColumns.add(inputColumn);
                    }

                    if (inputColumns.isEmpty()) {
                        continue;
                    }

                    Map<String, Object> columnLineageProcess = new HashMap<>();
                    columnLineageProcess.put(ATTRIBUTE_NAME, col.getToNameParse());
                    columnLineageProcess.put(ATTRIBUTE_QUALIFIED_NAME, outputColName);
                    columnLineageProcess.put(ATTRIBUTE_INPUTS, inputColumns);
                    columnLineageProcess.put(ATTRIBUTE_EXPRESSION, col.getColConditions());

                    ret.add(columnLineageProcess);
                }
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(e.getMessage(), e);
            }
        }
        return ret;
    }


    private boolean skipProcess() {
        Set<ReadEntity> inputs = getHiveContext().getInputs();
        Set<WriteEntity> outputs = getHiveContext().getOutputs();

        boolean ret = CollectionUtils.isEmpty(inputs) && CollectionUtils.isEmpty(outputs);

        if (!ret) {
            if (getContext().getHiveOperation() == HiveOperation.QUERY) {
                // Select query has only one output
                if (outputs.size() == 1) {
                    WriteEntity output = outputs.iterator().next();

                    if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR) {
                        if (output.getWriteType() == WriteEntity.WriteType.PATH_WRITE && output.isTempURI()) {
                            ret = true;
                        }
                    }

                }
            }
        }

        return ret;
    }
}
