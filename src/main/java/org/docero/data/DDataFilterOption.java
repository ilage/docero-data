package org.docero.data;

public enum DDataFilterOption {
    /** value equals to parameter */
    EQUALS,
    /** value not equals to parameter (or NULL) */
    NOT_EQUALS,
    /** value is lower than parameter */
    LOWER_THAN,
    /** value is greater than parameter */
    GREATER_THAN,
    /** value is greater or equals to parameter */
    NO_LOWER_THAN,
    /** value is lower or equals to parameter */
    NO_GREATER_THAN,
    /** value is null */
    IS_NULL,
    /** value not is null */
    NOT_IS_NULL,
    /** value is in array parameter */
    IN,
    /** value is in array parameter('%' and '_'  used ) */
    SIMILAR_TO,
    /** value contains passed string */
    LIKE,
    /** value contains any word in passed string */
    LIKE_HAS,
    /** value contains all words in passed string */
    LIKE_HAS_ALL,
    /** value starts with passed string */
    LIKE_STARTS,
    /** values ends by passed string */
    LIKE_ENDS,
    /** value contains passed string with ignore case */
    ILIKE,
    /** value contains any word in passed string with ignore case */
    ILIKE_HAS,
    /** value contains all words in passed string with ignore case */
    ILIKE_HAS_ALL,
    /** value starts with passed string with ignore case */
    ILIKE_STARTS,
    /** values ends by passed string with ignore case */
    ILIKE_ENDS
}
