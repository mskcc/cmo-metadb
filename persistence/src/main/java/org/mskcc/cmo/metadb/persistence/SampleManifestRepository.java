package org.mskcc.cmo.metadb.persistence;

import java.util.List;
import java.util.UUID;
import org.mskcc.cmo.metadb.model.SampleAlias;
import org.mskcc.cmo.metadb.model.SampleManifestEntity;
import org.mskcc.cmo.metadb.model.SampleManifestJsonEntity;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 *
 * @author ochoaa
 */

@Repository
public interface SampleManifestRepository extends Neo4jRepository<SampleManifestEntity, UUID> {
    @Query("MATCH (s: sample {value: $igoId.sampleId, idSource: 'igoId'}) "
        + "MATCH (s)-[:SP_TO_SP]->(sm) "
        + "RETURN sm")
    SampleManifestEntity findSampleByIgoId(@Param("igoId") SampleAlias igoId);

    @Query("MATCH (sm: cmo_metadb_sample_metadata {uuid: $uuid})"
            + "MATCH (sm)-[SP_TO_SP]->(s)"
            + "WHERE s.idSource = 'igoId' RETURN s;")
    SampleAlias findSampleIgoId(@Param("uuid") UUID uuid);

    @Query("MATCH (sm: cmo_metadb_sample_metadata {uuid: $uuid})"
            + "MATCH (sm)-[SP_TO_SP]->(s)"
            + "WHERE s.idSource = 'investigatorId' RETURN s;")
    SampleAlias findInvestigatorId(@Param("uuid") UUID uuid);

    @Query("MATCH (sm: cmo_metadb_sample_metadata{uuid: $uuid}) "
            + "MATCH (json)<-[r:SAMPLE_MANIFEST]-(sm) "
            + "DELETE r "
            + "CREATE (new_json: sample_manifest_json "
            + "{sampleManifestJson: $sampleManifestJson.sampleManifestJson}) "
            + "MERGE(sm)-[:SAMPLE_METADATA]->(new_json) "
            + "MERGE(new_json)-[:SAMPLE_METADATA]->(json)")
    SampleManifestEntity updateSampleManifestJson(
            @Param("sampleManifestJson")SampleManifestJsonEntity sampleManifestJson,
            @Param("uuid") UUID uuid);

    @Query("MATCH (s: cmo_metadb_sample_metadata{uuid: $sampleManifestEntity.uuid})"
            + "MATCH (s)<-[:PX_TO_SP]-(p:cmo_metadb_patient_metadata)"
            + "MATCH (n)<-[:PX_TO_SP]-(p) "
            + "WHERE n.tumorOrNormal = 'Normal'"
            + "RETURN n")
    List<SampleManifestEntity> findSamplesWithSamePatient(
            @Param("sampleManifestEntity") SampleManifestEntity sampleManifestEntity);

    @Query("MATCH (s: cmo_metadb_sample_metadata{uuid: $sampleManifestEntity.uuid}) "
            + "MATCH (s)<-[:REQUEST_TO_SP]-(r:cmo_metadb_request) "
            + "RETURN r.pooledNormals")
    List<String> findPooledNormals(@Param("sampleManifestEntity") SampleManifestEntity sampleManifestEntity);
}
