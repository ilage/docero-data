package org.docero.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface EnableDDataConfiguration {
    /**
     * Package name used for create classes DData,DDataResources,AbstractBean,AbstractRepository and
     * if spring available DDataConfiguration.
     * <p>if packageName specified it has precedence over packageClass</p>
     */
    String packageName() default "";

    /**
     * Class witch package used for create classes DData,DDataResources,AbstractBean,AbstractRepository and
     * if spring available DDataConfiguration.
     * <p>if packageName specified it has precedence over packageClass</p>
     */
    Class<?> packageClass() default EnableDDataConfiguration.class;

    /**
     * if spring available set name for DData component, it may be useful in @DependsOn annotation.
     */
    String springComponentName() default "";
}
