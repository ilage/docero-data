package org.docero.data.utils;

import org.docero.data.DData;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;

import java.util.Arrays;

public class DDataSpringResources {
    @SafeVarargs
    public static Resource[] get(ApplicationContext context, Class<? extends DDataModule>... modules) {
        return Arrays.stream(DData.resources(modules))
                .map(context::getResource)
                .toArray(Resource[]::new);
    }
}
