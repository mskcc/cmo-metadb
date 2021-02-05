package org.mskcc.cmo.metadb.model.neo4j;

import java.util.List;
import org.mskcc.cmo.metadb.model.Library;
import org.mskcc.cmo.metadb.model.QcReport;
import org.mskcc.cmo.metadb.model.SampleManifest;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;

@NodeEntity
public class SampleManifestEntity extends SampleManifest {
    @Id @GeneratedValue
    private Long id;
    private String sampleManifestJson;
    private String creationTime;
    private String qcReportsJson;
    private String librariesJson;

    public SampleManifestEntity() {}

    public SampleManifestEntity(String sampleManifestJson, String creationTime) {
        this.sampleManifestJson = sampleManifestJson;
        this.creationTime = creationTime;
    }

    /**
     * SampleManifestEntity constructor.
     * @param sampleManifest
     */
    public SampleManifestEntity(SampleManifest sampleManifest) {
        this.mrn = sampleManifest.getMrn();
        this.cmoPatientId = sampleManifest.getCmoPatientId();
        this.cmoSampleId = sampleManifest.getCmoSampleId();
        this.igoId = sampleManifest.getIgoId();
        this.investigatorSampleId = sampleManifest.getInvestigatorSampleId();
        this.species = sampleManifest.getSpecies();
        this.sex = sampleManifest.getSex();
        this.tumorOrNormal = sampleManifest.getTumorOrNormal();
        this.sampleType = sampleManifest.getSampleType();
        this.preservation = sampleManifest.getPreservation();
        this.tumorType = sampleManifest.getTumorType();
        this.parentTumorType = sampleManifest.getParentTumorType();
        this.specimenType = sampleManifest.getSpecimenType();
        this.sampleOrigin = sampleManifest.getSampleOrigin();
        this.tissueSource = sampleManifest.getTissueSource();
        this.tissueLocation = sampleManifest.getTissueLocation();
        this.recipe = sampleManifest.getRecipe();
        this.baitset = sampleManifest.getBaitset();
        this.fastqPath = sampleManifest.getFastqPath();
        this.principalInvestigator = sampleManifest.getPrincipalInvestigator();
        this.ancestorSample = sampleManifest.getAncestorSample();
        this.doNotUse = sampleManifest.isDoNotUse();
        this.sampleStatus = sampleManifest.getSampleStatus();
        this.cmoInfoIgoId = sampleManifest.getCmoInfoIgoId();
        this.cmoSampleName = sampleManifest.getCmoSampleName();
        this.sampleName = sampleManifest.getSampleName();
        this.cmoSampleClass = sampleManifest.getCmoSampleClass();
        this.oncotreeCode = sampleManifest.getOncotreeCode();
        this.collectionYear = sampleManifest.getCollectionYear();
        this.tubeId = sampleManifest.getTubeId();
        this.cfDNA2dBarcode = sampleManifest.getCfDNA2dBarcode();
    }

    /**
     * SampleManifestEntity constructor
     * @param igoId
     * @param cmoInfoIgoId
     * @param cmoSampleName
     * @param sampleName
     * @param cmoSampleClass
     * @param cmoPatientId
     * @param investigatorSampleId
     * @param oncotreeCode
     * @param tumorOrNormal
     * @param tissueLocation
     * @param specimenType
     * @param sampleOrigin
     * @param preservation
     * @param collectionYear
     * @param sex
     * @param species
     * @param tubeId
     * @param cfDNA2dBarcode
     * @param baitSet
     * @param qcReports
     * @param libraries
     * @param sampleManifestJson
     * @param creationTime
     */
    public SampleManifestEntity(String igoId, String cmoInfoIgoId, String cmoSampleName, String sampleName,
            String cmoSampleClass, String cmoPatientId, String investigatorSampleId, String oncotreeCode,
            String tumorOrNormal, String tissueLocation, String specimenType, String sampleOrigin,
            String preservation, String collectionYear, String sex, String species, String tubeId,
            String cfDNA2dBarcode, String baitSet, List<QcReport> qcReports, List<Library> libraries, 
            String mrn, String cmoSampleId, String sampleType, String tumorType, String parentTumorType, 
            String tissueSource, String recipe, String baitset, String fastqPath, 
            String principalInvestigator, String ancestorSample, Boolean doNotUse,
            String sampleStatus, String sampleManifestJson, String creationTime) {
        super(igoId, cmoInfoIgoId, cmoSampleName, sampleName,
                cmoSampleClass, cmoPatientId, investigatorSampleId, oncotreeCode,
                tumorOrNormal, tissueLocation, specimenType, sampleOrigin,
                preservation, collectionYear, sex, species, tubeId,
                cfDNA2dBarcode, baitSet, qcReports, libraries, 
                mrn, cmoSampleId, sampleType, tumorType, parentTumorType, 
                tissueSource, recipe, baitset, fastqPath, 
                principalInvestigator, ancestorSample, doNotUse,
                sampleStatus);
        this.sampleManifestJson = sampleManifestJson;
        this.creationTime = creationTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSampleManifestJson() {
        return sampleManifestJson;
    }

    public void setSampleManifestJson(String sampleManifestJson) {
        this.sampleManifestJson = sampleManifestJson;
    }

    public String getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(String creationDate) {
        this.creationTime = creationTime;
    }

    public String getQcReportsJson() {
        return qcReportsJson;
    }

    public void setQcReportsJson(String qcReportsJson) {
        this.qcReportsJson = qcReportsJson;
    }

    public String getLibrariesJson() {
        return librariesJson;
    }

    public void setLibrariesJson(String librariesJson) {
        this.librariesJson = librariesJson;
    }
}
