package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;
import org.docero.data.DDataVersionalBean;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Created by i.vasyashin on 26.07.2017.
 */
@DDataBean(schema = "public", table = "objectsediting")
public interface BuildingHE extends DDataVersionalBean<LocalDateTime> {
    @DDataProperty(value = "id")
    long getId();

    void setId(long id);

    /*ObjectsClasses getClassType();
    void setClassType(ObjectsClasses classType);*/

    @DDataProperty("cadastralblock")
    String getCadastralblock();

    void setCadastralblock(String cadastralblock);

    @DDataProperty("guid")
    String getGuid();

    void setGuid(String guid);

    @DDataProperty(value = "cadastralnumber", id = true)
    String getCadastralNumber();

    void setCadastralNumber(String cadastralNumber);

    @DDataProperty(value = "datecreated", versionFrom = true, id = true)
    LocalDateTime getDateCreated();

    void setDateCreated(LocalDateTime dateCreated);

    /*KindCadastralObjects getObjectType();

    void setObjectType(KindCadastralObjects objecttype);

    PurposeOfBuildings getAssignationBuilding();

    void setAssignationBuilding(PurposeOfBuildings assignationBuilding);*/

    default LocalDate getCreatedLocalDate() {
        return getDateCreated().toLocalDate();
    }

    default void setCreatedLocalDate(LocalDate value) {
        setDateCreated(value.atStartOfDay());
    }

    //do not add Mapping - check compilation errors
    BuildingHE getParent();

    void setParent(BuildingHE val);
}
