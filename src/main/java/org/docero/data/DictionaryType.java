package org.docero.data;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

@XmlType(name = "dictionaryType", namespace = "http://data.docero.org/")
@XmlEnum
public enum DictionaryType {
    NO, SMALL, LARGE
}
