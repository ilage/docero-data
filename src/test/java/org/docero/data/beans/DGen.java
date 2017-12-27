package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;
import org.docero.data.GeneratedValue;
import org.docero.dgen.DGenInterface;

import java.io.Serializable;

@SuppressWarnings("unused")
abstract class DGen {
    @DGenInterface
    @DDataBean
    abstract class SampleTable implements Serializable {
        @GeneratedValue("ddata.sample_seq")
        @DDataProperty(value = "id", id = true)
        int id;
        @DDataProperty(value = "s", nullable = false)
        String strParameter;
        @DDataProperty("i")
        int innerId;
    }
    @DGenInterface
    @DDataBean
    abstract class SmallDictTable implements Serializable {
        @GeneratedValue("ddata.sample_seq")
        @DDataProperty(value = "id", id = true)
        int id;
        @DDataProperty(value = "name", nullable = false)
        String name;
    }
    @DGenInterface
    @DDataBean
    abstract class LargeDictTable implements Serializable {
        @GeneratedValue("ddata.sample_seq")
        @DDataProperty(value = "id", id = true)
        int id;
        @DDataProperty(value = "name", nullable = false)
        String name;
    }
}

