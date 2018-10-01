/*
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

package dev.at.mikorn.nifi.processors.mongodb.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.bson.conversions.Bson;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"mongo", "aggregation", "aggregate"})
@CapabilityDescription("A processor that runs an aggregation query whenever a flowfile is received.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
public class MikornRunMongoAggregation extends AbstractMongoProcessor {

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .description("The input flowfile gets sent to this relationship when the query succeeds.")
            .name("original")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("The input flowfile gets sent to this relationship when the query fails.")
            .name("failure")
            .build();
    static final Relationship REL_RESULTS = new Relationship.Builder()
            .description("The result set of the aggregation will be sent to this relationship.")
            .name("results")
            .build();

    static final List<Bson> buildAggregationQuery(String query) throws IOException {
        List<Bson> result = new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        List<Map> values = mapper.readValue(query, List.class);
        for (Map val : values) {
            result.add(new BasicDBObject(val));
        }

        return result;
    }

    public static final Validator AGG_VALIDATOR = (subject, value, context) -> {
        final ValidationResult.Builder builder = new ValidationResult.Builder();
        builder.subject(subject).input(value);

        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return builder.valid(true).explanation("Contains Expression Language").build();
        }

        String reason = null;
        try {
            buildAggregationQuery(value);
        } catch (final RuntimeException | IOException e) {
            reason = e.getLocalizedMessage();
        }

        return builder.explanation(reason).valid(reason == null).build();
    };

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("mongo-agg-query")
            .displayName("Query")
            .defaultValue("query1")
            .expressionLanguageSupported(true)
            .description("The aggregation query to be executed.")
            .required(true)
//            .addValidator(AGG_VALIDATOR)
            .build();

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(QUERY_ATTRIBUTE);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_RESULTS);
        _relationships.add(REL_ORIGINAL);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    static String buildBatch(List batch) {
        ObjectMapper mapper = new ObjectMapper();
        String retVal;
        try {
            retVal = mapper.writeValueAsString(batch.size() > 1 ? batch : batch.get(0));
        } catch (Exception e) {
            retVal = null;
        }

        return retVal;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = null;
        String lastTime = flowFile.getAttribute("last_time");
        String query1 = String.format("[\n" +
                "{\n" +
                "\t\"$project\": {\n" +
                "\t\t\"_id\": 1,\n" +
                "\t\t\"task_name\": 1,\n" +
                "\t\t\"task_desc\": 1,\n" +
                "\t\t\"project\": 1,\n" +
                "\t\t\"assigner\": 1,\n" +
                "\t\t\"status\": 1,\n" +
                "\t\t\"assigned_on\": 1,\n" +
                "\t\t\"resources\": 1,\n" +
                "\t\t\"annotation_type\": 1,\n" +
                "\t\t\"start_on\":1,\n" +
                "\t\t\"finish_on\": 1,\n" +
                "\t\t\"real_start_time\": 1,\n" +
                "        \"real_completed_time\": 1,\n" +
                "\t\t\"files\": {\n" +
                "\t\t\t\"$filter\": {\n" +
                "\t\t\t\t\"input\": \"$files\",\n" +
                "\t\t\t\t\"as\": \"file\",\n" +
                "\t\t\t\t\"cond\": {\n" +
                "\t\t\t\t\t\"$gte\": [\"$$file.last_modified_on\", {\n" +
                "\t\t\t\t\t\t\"$dateFromString\": {\n" +
                "\t\t\t\t\t\t\t\"dateString\": \"%s\"\n" +
                "\t\t\t\t\t\t}\n" +
                "\t\t\t\t\t}]\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "},\n" +
                "    {\n" +
                "        \"$lookup\": {\n" +
                "            \"from\": \"projects\",\n" +
                "            \"localField\": \"project._id\",\n" +
                "            \"foreignField\": \"_id\",\n" +
                "            \"as\": \"project\"\n" +
                "\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"$unwind\": \"$project\"\n" +
                "    },\n" +
                "\n" +
                "    {\n" +
                "        \"$unwind\": \"$files\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"$replaceRoot\": {\n" +
                "            \"newRoot\": {\n" +
                "                \"$mergeObjects\": [\"$files\", \"$$ROOT\"]\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"$project\": {\n" +
                "            \"_id\": {\n" +
                "                \"$concat\": [\"$project._id\", \"$_id\", \"$files.file_id\"]\n" +
                "            },\n" +
                "            \"project\": {\n" +
                "                \"_id\": \"$project._id\",\n" +
                "                \"project_name\": \"$project.project_name\",\n" +
                "                \"project_desc\": \"$project.project_desc\",\n" +
                "                \"project_type\": \"$project.project_type\",\n" +
                "                \"client_name\": \"$project.client_name\",\n" +
                "                \"status\": \"$project.status\",\n" +
                "                \"start_on\": \"$project.start_on\",\n" +
                "                \"finish_on\": \"$project.finish_on\"\n" +
                "\n" +
                "            },\n" +
                "            \"task\": {\n" +
                "                \"_id\": \"$_id\",\n" +
                "                \"task_name\": \"$task_name\",\n" +
                "                \"status\": \"$status\",\n" +
                "                \"assigner\": \"$assigner\",\n" +
                "                \"assigned_on\": \"$assigned_on\",\n" +
                "                \"annotation_type\": \"$annotation_type\",\n" +
                "                \"start_on\": \"$start_on\",\n" +
                "                \"finish_on\": \"$finish_on\",\n" +
                "                \"real_start_time\": \"$real_start_time\",\n" +
                "                \"real_completed_time\":\"$real_completed_time\"\n" +
                "            },\n" +
                "            \"filed_id\": 1,\n" +
                "            \"file_name\": \"$file_patch_name\",\n" +
                "            \"file_size\": \"$file_patch_size_mb\",\n" +
                "            \"working_version\": 1,\n" +
                "            \"status\": 1,\n" +
                "            \"received_on\": 1,\n" +
                "            \"total_unit\": 1,\n" +
                "            \"report_fields\": 1,\n" +
                "            \"actual_start_annotate\": 1,\n" +
                "            \"actual_complete_annotate\": 1,\n" +
                "            \"actual_start_check\": 1,\n" +
                "            \"actual_complete_check\": 1,\n" +
                "            \"actual_completed\": 1,\n" +
                "            \"annotate_duration\": 1,\n" +
                "            \"check_duration\": 1\n" +
                "        }\n" +
                "    }\n" +
                "]", lastTime);

        String query4 = String.format("[{\n" +
                "        \"$addFields\": {\n" +
                "            \"filter_cond\": {\n" +
                "                \"$dateFromString\": {\n" +
                "                    \"dateString\": \"%s\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "\n" +
                "    },\n" +
                "    {\n" +
                "        \"$unwind\": \"$files\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"$project\": {\n" +
                "            \"_id\": 1,\n" +
                "            \"task_name\": 1,\n" +
                "            \"project\": 1,\n" +
                "            \"annotation_type\": 1,\n" +
                "            \"file_id\": \"$files.file_id\",\n" +
                "            \"file_name\": \"$files.file_patch_name\",\n" +
                "            \"working_version\": \"$files.working_version\",\n" +
                "            \"file_size\": \"$files.file_patch_size_mb\",\n" +
                "            \"total_unit\": \"$files.total_unit\",\n" +
                "                        \"filter_cond\": 1,\n" +
                "            \"tickets\": {\n" +
                "                \"$filter\": {\n" +
                "                    \"input\": \"$files.tickets\",\n" +
                "                    \"as\": \"ticket\",\n" +
                "                    \"cond\": {\n" +
                "                        \"$or\": [{\n" +
                "                                \"$gte\": [\"$$ticket.last_annotating\", \"$filter_cond\"]\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"$gte\": [\"$$ticket.last_checking\", \"$filter_cond\"]\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "    \n" +
                "       {\n" +
                "            \"$lookup\": {\n" +
                "                \"from\": \"projects\",\n" +
                "                \"localField\": \"project._id\",\n" +
                "                \"foreignField\": \"_id\",\n" +
                "                \"as\": \"project\"\n" +
                "\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"$unwind\": \"$project\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"$unwind\": \"$tickets\"\n" +
                "        },\n" +
                "     {\n" +
                "            \"$addFields\": {\n" +
                "                \"date\": {\n" +
                "                    \"$dateFromParts\": {\n" +
                "                        \"year\": {\n" +
                "                            \"$year\": \"$filter_cond\"\n" +
                "                        },\n" +
                "                        \"month\": {\n" +
                "                            \"$month\": \"$filter_cond\"\n" +
                "                        },\n" +
                "                        \"day\": {\n" +
                "                            \"$dayOfMonth\": \"$filter_cond\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "\n" +
                "        },\n" +
                "      {\n" +
                "          \"$project\": {\n" +
                "              \"task._id\": \"$_id\",\n" +
                "              \"task.task_name\": \"$task_name\",\n" +
                "              \"task.annotation_type\": \"$annotation_type\",\n" +
                "              \"project._id\": 1,\n" +
                "              \"project.project_name\": 1,\n" +
                "              \"project.project_type\": 1,\n" +
                "              \"project.client_name\": 1,\n" +
                "              \"file_id\": 1,\n" +
                "              \"file_name\": 1,\n" +
                "              \"working_version\": 1,\n" +
                "              \"file_size\": 1,\n" +
                "              \"total_unit\": 1,\n" +
                "              \"ticket_id\": \"$tickets.ticket_id\",\n" +
                "              \"round\": \"$tickets.round\",\n" +
                "              \"level\": \"$tickets.level\",\n" +
                "              \"priority_level\": \"$tickets.priority_level\",\n" +
                "                  \"date\": \"$date\",\n" +
                "              \"tmp\": [{\n" +
                "                      \"action_type\": \"annotating\",\n" +
                "                      \"user\": \"$tickets.annotator\",\n" +
                "                      \"events\": {\n" +
                "                          \"$filter\": {\n" +
                "                              \"input\": \"$tickets.annotate_events\",\n" +
                "                              \"as\": \"item\",\n" +
                "                              \"cond\": {\n" +
                "                                  \"$gte\": [\"$$item.start_annotating\", \"$date\"]\n" +
                "                              }\n" +
                "                          }\n" +
                "                      }\n" +
                "                  },\n" +
                "                  {\n" +
                "                      \"action_type\": \"checking\",\n" +
                "                      \"user\": \"$tickets.checker\",\n" +
                "                      \"events\": {\n" +
                "                          \"$filter\": {\n" +
                "                              \"input\": \"$tickets.check_events\",\n" +
                "                              \"as\": \"item\",\n" +
                "                              \"cond\": {\n" +
                "                                  \"$gte\": [\"$$item.start_checking\", \"$date\"]\n" +
                "                              }\n" +
                "                          }\n" +
                "                      }\n" +
                "                  }\n" +
                "              ]\n" +
                "          }\n" +
                "      },\n" +
                "      {\n" +
                "          \"$unwind\": \"$tmp\"\n" +
                "      },\n" +
                "     {\n" +
                "          \"$unwind\": \"$tmp.events\"\n" +
                "      },\n" +
                "\n" +
                " {\n" +
                "          \"$group\": {\n" +
                "              \"_id\": {\n" +
                "                  \"_id\": \"$_id\",\n" +
                "                  \"project\": \"$project\",\n" +
                "                  \"task\": \"$task\",\n" +
                "                  \"file_id\": \"$file_id\",\n" +
                "                  \"file_name\": \"$file_name\",\n" +
                "                  \"file_size\": \"$file_size\",\n" +
                "                  \"ticket_id\":  \"$ticket_id\",\n" +
                "                  \"round\":\"$round\",\n" +
                "                  \"level\":  \"$level\",\n" +
                "                  \"priority_level\":  \"$priority_level\",\n" +
                "                  \"action_type\": \"$tmp.action_type\",\n" +
                "                  \"user\": \"$tmp.user\",\n" +
                "                  \"date\": {\n" +
                "                      \"$cond\": [{\n" +
                "                             \"$eq\": [\"$tmp.action_type\",\"annotating\" ]\n" +
                "                          }, {\n" +
                "                              \"$dateFromParts\": {\n" +
                "                                  \"year\": {\n" +
                "                                      \"$year\": \"$tmp.events.start_annotating\"\n" +
                "                                  },\n" +
                "                                  \"month\": {\n" +
                "                                      \"$month\": \"$tmp.events.start_annotating\"\n" +
                "                                  },\n" +
                "                                  \"day\": {\n" +
                "                                      \"$dayOfMonth\": \"$tmp.events.start_annotating\"\n" +
                "                                  }\n" +
                "                              }\n" +
                "                          },\n" +
                "                          {\n" +
                "                              \"$dateFromParts\":\n" +
                "                              {\n" +
                "                                  \"year\": {\n" +
                "                                      \"$year\": \"$tmp.events.start_checking\"\n" +
                "                                  },\n" +
                "                                  \"month\": {\n" +
                "                                      \"$month\": \"$tmp.events.start_checking\"\n" +
                "                                  },\n" +
                "                                  \"day\": {\n" +
                "                                      \"$dayOfMonth\": \"$tmp.events.start_checking\"\n" +
                "                                  }\n" +
                "                              }\n" +
                "                          }\n" +
                "                      ]\n" +
                "                  }\n" +
                "              },\n" +
                "              \"duration\": {\n" +
                "                  \"$sum\": \"$tmp.events.duration\"\n" +
                "              },\n" +
                "              \"completed_unit\": {\n" +
                "                  \"$sum\": \"$tmp.events.completed_unit\"\n" +
                "              }\n" +
                "          }\n" +
                "      },\n" +
                "     {\n" +
                "          \"$project\": {\n" +
                "              \"_id\": {\n" +
                "                  \"$concat\": [\"$_id.project._id\", \"-\", \"$_id.task._id\", \"-\",\n" +
                "                      {\n" +
                "                          \"$substr\": [\"$_id.ticket_id\", 0, 4]\n" +
                "                      }, \"-\",\n" +
                "                      {\n" +
                "                          \"$substr\": [\"$_id.round\", 0, 4]\n" +
                "                      }, \"-\", \"$_id.user._id\", \"-\",\n" +
                "                      {\n" +
                "                          \"$dateToString\": {\n" +
                "                              \"date\": \"$_id.date\"\n" +
                "                          }\n" +
                "                      }\n" +
                "                  ]\n" +
                "              },\n" +
                "              \"project\": \"$_id.project\",\n" +
                "              \"task\": \"$_id.task\",\n" +
                "              \"file_id\": \"$_id.file_id\",\n" +
                "              \"file_name\": \"$_id.file_name\",\n" +
                "              \"file_size\": \"$_id.file_size\",\n" +
                "              \"ticket_id\": \"$_id.ticket_id\",\n" +
                "              \"round\": \"$_id.round\",\n" +
                "              \"level\": \"$_id.level\",\n" +
                "              \"priority_level\": \"$_id.priority_level\",\n" +
                "              \"action_type\": \"$_id.action_type\",\n" +
                "              \"user\": \"$_id.user\",\n" +
                "              \"@date\": \"$_id.date\",\n" +
                "              \"duration\": 1,\n" +
                "              \"completed_unit\": 1\n" +
                "          }\n" +
                "      }\n" +
                "]" , lastTime);


        if (context.hasIncomingConnection()) {
            flowFile = session.get();

            if (flowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
        String queryAttr = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        Integer batchSize = context.getProperty(BATCH_SIZE).asInteger();
        Integer resultsPerFlowfile = context.getProperty(RESULTS_PER_FLOWFILE).asInteger();

        Map attrs = new HashMap();
        if (queryAttr != null && queryAttr.trim().length() > 0) {
            attrs.put(queryAttr, query);
        }

        MongoCollection collection = getCollection(context);
        MongoCursor iter = null;

        try {
            List<Bson> aggQuery = new ArrayList<>();
            if (query.equalsIgnoreCase("[{\"query\":\"1\"}]")) {
                aggQuery = buildAggregationQuery(query1);
            } else if( query.equalsIgnoreCase("[{\"query\":\"2\"}]")) {
                aggQuery = buildAggregationQuery(query4);
            } else {
                aggQuery = buildAggregationQuery(query);
            }

            AggregateIterable it = collection.aggregate(aggQuery);
            it.batchSize(batchSize != null ? batchSize : 1);

            iter = it.iterator();
            List batch = new ArrayList();

            while (iter.hasNext()) {
                batch.add(iter.next());
                if (batch.size() == resultsPerFlowfile) {
                    writeBatch(buildBatch(batch), flowFile, context, session, attrs, REL_RESULTS);
                    batch = new ArrayList();
                }
            }

            if (batch.size() > 0) {
                writeBatch(buildBatch(batch), flowFile, context, session, attrs, REL_RESULTS);
            }

            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }
        } catch (Exception e) {
            getLogger().error("Error running MongoDB aggregation query.", e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }
}
