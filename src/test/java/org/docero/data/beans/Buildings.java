package org.docero.data.beans;

import org.docero.data.DDataRep;
import org.docero.data.DDataVersionalRepository;
import org.docero.data.SelectId;

import java.time.LocalDateTime;

@DDataRep
public interface Buildings extends DDataVersionalRepository<BuildingHE, String, LocalDateTime> {
    @Override
    @SelectId("org.docero.data.example.MyMapping.selectBuilding")
    BuildingHE get(String cadNum, LocalDateTime varDate);
}
