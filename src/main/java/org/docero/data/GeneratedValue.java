package org.docero.data;

public @interface GeneratedValue {
    GenerationType strategy() default GenerationType.SEQUENCE;
    String value() default "";
    /**
     * Synonym for value
     * @return value
     */
    String generator() default "";
    /**
     * If set to FALSE, it runs the insert statement and then
     * the selectKey statement – which is common with databases
     * like Oracle that may have embedded sequence calls
     * inside of insert statements.
     *
     * @return do generation before insert
     */
    boolean before() default true;
}
