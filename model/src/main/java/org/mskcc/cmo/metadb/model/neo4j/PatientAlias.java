package org.mskcc.cmo.metadb.model.neo4j;

import java.io.Serializable;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.Relationship;

/**
 *
 * @author ochoaa
 */

@NodeEntity
public class PatientAlias implements Serializable {
    @Id @GeneratedValue
    private Long id;
    @Property(name = "value")
    private String patientId;
    private String idSource;
    @Relationship(type = "IS_ALIAS", direction = Relationship.OUTGOING)
    private MetaDbPatient metaDbPatient;

    public PatientAlias() {}

    public PatientAlias(String patientId, String idSource) {
        this.patientId = patientId;
        this.idSource = idSource;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public String getIdSource() {
        return idSource;
    }

    public void setIdSource(String idSource) {
        this.idSource = idSource;
    }

    public MetaDbPatient getPatientMetadata() {
        return metaDbPatient;
    }

    public void setPatientMetadata(MetaDbPatient metaDbPatient) {
        this.metaDbPatient = metaDbPatient;
    }

}
