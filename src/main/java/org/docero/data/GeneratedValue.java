package org.docero.data;

public @interface GeneratedValue {
    GenerationType strategy() default GenerationType.SEQUENCE;
    String value() default "";
    /**
     * Synonym for value
     * @return value
     */
    String generator() default "";
}
