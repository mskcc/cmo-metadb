package org.mskcc.cmo.metadb.persistence;

import java.util.List;
import java.util.UUID;
import org.mskcc.cmo.metadb.model.MetaDbPatient;
import org.mskcc.cmo.metadb.model.MetaDbSample;
import org.mskcc.cmo.metadb.model.SampleAlias;
import org.mskcc.cmo.metadb.model.SampleManifestEntity;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 *
 * @author ochoaa
 */

@Repository
public interface MetaDbSampleRepository extends Neo4jRepository<MetaDbSample, UUID> {
    @Query("MATCH (sm: Sample {metaDbSampleId: $metaDbSampleId}) "
            + "RETURN sm")
    MetaDbSample findSampleByUUID(@Param("metaDbSampleId") UUID metaDbSampleId);

    @Query("MATCH (s: SampleAlias {value: $igoId.sampleId, namespace: 'igoId'}) "
        + "MATCH (s)<-[:IS_ALIAS]-(sm: Sample) "
        + "RETURN sm")
    MetaDbSample findSampleByIgoId(@Param("igoId") SampleAlias igoId);

    @Query("MATCH (sm: Sample {metaDbSampleId: $metaDbSampleId})"
            + "MATCH (sm)<-[:IS_ALIAS]-(s: SampleAlias)"
            + "WHERE toLower(s.namespace) = 'igoid' RETURN s;")
    SampleAlias findSampleIgoId(@Param("metaDbSampleId") UUID metaDbSampleId);

    @Query("MATCH (sm: Sample {metaDbSampleId: $metaDbSampleId})"
            + "MATCH (sm)<-[:IS_ALIAS]-(s: SampleAlias)"
            + "WHERE s.namespace = 'investigatorId' RETURN s;")
    SampleAlias findInvestigatorId(@Param("metaDbSampleId") UUID metaDbSampleId);

    @Query("MATCH (sm: Sample {metaDbSampleId: $metaDbSampleId})"
            + "MATCH (sm)<-[:HAS_SAMPLE]-(p: Patient)"
            + "RETURN p;")
    MetaDbPatient findPatientbyUUID(@Param("metaDbSampleId") UUID metaDbSampleId);

    @Query("MATCH (sm: Sample {metaDbSampleId: $metaDbSampleId})"
            + "MATCH (sm)-[:HAS_METADATA]->(s: SampleMetadata)"
            + "RETURN s;")
    List<SampleManifestEntity> findSampleManifestList(@Param("metaDbSampleId") UUID metaDbSampleId);

    @Query("MATCH (s: Sample {metaDbSampleId: $metaDbSample.metaDbSampleId})"
            + "MATCH (s)<-[:HAS_SAMPLE]-(p: Patient)"
            + "MATCH (n: Sample)<-[:HAS_SAMPLE]-(p) "
            + "WHERE toLower(n.sampleClass) = 'normal'"
            + "RETURN n")
    List<MetaDbSample> findMatchedNormals(
            @Param("metaDbSample") MetaDbSample metaDbSample);

    @Query("MATCH (s: Sample {metaDbSampleId: $metaDbSample.metaDbSampleId}) "
            + "MATCH (s)<-[:HAS_SAMPLE]-(r: Request)"
            + "RETURN r.pooledNormals")
    List<String> findPooledNormals(@Param("metaDbSample") MetaDbSample metaDbSample);

    @Query("MATCH (s: Sample {metaDbSampleId: $metaDbSampleId}) "
            + "MATCH (s)<-[:HAS_SAMPLE]-(p: Patient) "
            + "RETURN p.metaDbPatientId")
    UUID findPatientUuid(@Param("metaDbSampleId") UUID metaDbSampleId);
}
