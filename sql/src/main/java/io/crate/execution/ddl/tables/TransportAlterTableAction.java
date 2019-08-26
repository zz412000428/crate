/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl.tables;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.AlterTableClusterStateExecutor;
import io.crate.metadata.cluster.DDLClusterStateService;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportAlterTableAction extends AbstractDDLTransportAction<AlterTableRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/alter";
    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);

    private final AlterTableClusterStateExecutor executor;

    @Inject
    public TransportAlterTableAction(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     IndexNameExpressionResolver indexNameExpressionResolver,
                                     MetaDataMappingService metaDataMappingService,
                                     IndicesService indicesService,
                                     DDLClusterStateService ddlClusterStateService) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              indexNameExpressionResolver,
              AlterTableRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
            "alter-table");
        executor = new AlterTableClusterStateExecutor(metaDataMappingService, indicesService, indexNameExpressionResolver);
    }

    @Override
    public ClusterStateTaskExecutor<AlterTableRequest> clusterStateTaskExecutor(AlterTableRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(AlterTableRequest request, ClusterState state) {
        try {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, request.tableIdent().indexNameOrAlias()));
        } catch (IndexNotFoundException e) {
            if (request.isPartitioned() == false) {
                throw e;
            }
            // empty partition, no indices just a template exists.
            return null;
        }
    }
}
