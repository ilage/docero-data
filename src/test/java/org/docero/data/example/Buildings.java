package org.docero.data.example;

import org.docero.data.DDataRep;
import org.docero.data.DDataVersionalRepository;

import java.time.LocalDateTime;

@DDataRep
public interface Buildings extends DDataVersionalRepository<BuildingHE, String, LocalDateTime> {
    @Buildings_DDataFetch_(
            select = "SELECT * FROM ActualData(:cadNum,:varDate )",
            resultMap = "org.docero.data.example.MyMapping.getBuilding"
    )
    @Override
    BuildingHE get(String cadNum, LocalDateTime varDate);
}
