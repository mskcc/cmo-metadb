package org.mskcc.cmo.metadb.service;

import java.util.List;
import org.mskcc.cmo.metadb.model.neo4j.MetaDbSample;

public interface SampleService {

    MetaDbSample saveSampleManifest(MetaDbSample metaDbSample) throws Exception;

    MetaDbSample setUpMetaDbSample(MetaDbSample metaDbSample) throws Exception;

    MetaDbSample setUpSampleManifestEntity(MetaDbSample metaDbSample) throws Exception;

    List<MetaDbSample> findMatchedNormalSample(MetaDbSample metaDbSample)
            throws Exception;

    List<String> findPooledNormalSample(MetaDbSample metaDbSample) throws Exception;
}
