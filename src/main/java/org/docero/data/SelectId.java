package org.docero.data;

import java.lang.annotation.*;

@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD})
public @interface SelectId {
    String value();
}
