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

package io.crate.metadata.cluster;

import io.crate.Constants;
import io.crate.execution.ddl.tables.AlterTableRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;

public class AlterTableClusterStateExecutor extends DDLClusterStateTaskExecutor<AlterTableRequest> {

    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(
        false, false, true, true);

    private final MetaDataMappingService metaDataMappingService;
    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public AlterTableClusterStateExecutor(MetaDataMappingService metaDataMappingService,
                                          IndicesService indicesService,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        this.metaDataMappingService = metaDataMappingService;
        this.indicesService = indicesService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, AlterTableRequest request) throws Exception {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(currentState, INDICES_OPTIONS, request.tableIdent().indexNameOrAlias());
        if (request.isPartitioned()) {
            if (request.partitionIndexName() != null) {
                concreteIndices = indexNameExpressionResolver.concreteIndices(currentState, INDICES_OPTIONS, request.partitionIndexName());
                currentState = updateMapping(currentState, request, concreteIndices);
                currentState = updateSettings(currentState, request);
            } else {
                // template gets all changes unfiltered
                currentState = updateTemplate(currentState, request);

                if (!request.excludePartitions()) {
                    currentState = updateSettings(currentState, request);
                    currentState = updateMapping(currentState, request, concreteIndices);
                }
            }
        } else {
            currentState = updateMapping(currentState, request, concreteIndices);
            currentState = updateSettings(currentState, request);
        }

        return currentState;
    }

    private ClusterState updateMapping(ClusterState currentState, AlterTableRequest request, Index[] concreteIndices) throws Exception {
        if (request.source() == null) {
            return currentState;
        }
        Map<Index, MapperService> indexMapperServices = new HashMap<>();
        for (Index index : concreteIndices) {
            final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
            if (indexMapperServices.containsKey(indexMetaData.getIndex()) == false) {
                MapperService mapperService = indicesService.createIndexMapperService(indexMetaData);
                indexMapperServices.put(index, mapperService);
                // add mappings for all types, we need them for cross-type validation
                mapperService.merge(indexMetaData, MapperService.MergeReason.MAPPING_RECOVERY, false);
            }
        }

        PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest()
            .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
            .indices(concreteIndices).type(Constants.DEFAULT_MAPPING_TYPE)
            .source(request.source());

        return metaDataMappingService.putMappingExecutor.applyRequest(currentState, updateRequest, indexMapperServices);
    }

    /**
     * FIXME: implement update settings by copying over the relevant code of Elasticsearch as using ES components
     * is not possible here.
     * See {@link MetaDataUpdateSettingsService#updateSettings(UpdateSettingsClusterStateUpdateRequest, ActionListener)}
     */
    private ClusterState updateSettings(ClusterState currentState, AlterTableRequest request) {
        return currentState;
    }

    /**
     * FIXME: implement template updating which operates directly on the cluster state.
     */
    private ClusterState updateTemplate(ClusterState currentState, AlterTableRequest request) {
        return currentState;
    }
}
