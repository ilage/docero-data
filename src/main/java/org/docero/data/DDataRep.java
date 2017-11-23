package org.docero.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface DDataRep {
    /**
     * class simple name for implementation
     * @return name
     */
    String value() default "";
    /**
     * Array of classes for multi-bean repository (like a DDataBatchOpsRepository)
     * @return array of repositories
     */
    Class[] beans() default {};
    /**
     * Array of DDataDiscriminator with pairs of values and bean-interfaces.<br>
     * Used with @&lt;RepositoryInterface&gt;_Discriminator_ generic annotation, what defines column used for discriminator.
     * @return array of discriminator mapping to discriminator value
     */
    DDataDiscriminator[] discriminator() default {};
}
