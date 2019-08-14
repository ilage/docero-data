package org.docero.data.utils;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * used by the 'save' method of the * _Dao_ class to include and exclude attributes,
 * as well as preprocessing bins before using the 'save' method
 */
public class UpdateOptions {
    private boolean isIncludeJsonProps;
    private boolean isIncludeXmlProps;
    private ArrayList<DDataAttribute> excluded = new ArrayList<>();
    private Map<Class, Consumer> beanHandlers = new HashMap<>();

    private UpdateOptions(boolean isIncludeJsonProps, boolean isIncludeXmlProps) {
        this.isIncludeJsonProps = isIncludeJsonProps;
        this.isIncludeXmlProps = isIncludeXmlProps;
    }

    /**
     * builded and returned new instance of UpdateOptions class
     *
     * @return new instance of UpdateOptions
     */
    public static UpdateOptions build() {
        return new UpdateOptions(false, false);
    }

    /**
     * included attributes marked JsonIgnore annotation to saved
     *
     * @return current instance UpdateOptions
     */
    public UpdateOptions includeJsonProps() {
        isIncludeJsonProps = true;
        return this;
    }

    /**
     * included attributes marked XmlTransient annotation to saved
     *
     * @return current instance UpdateOptions
     */
    public UpdateOptions includeXmlProps() {
        isIncludeXmlProps = true;
        return this;
    }
    /**
     * @param attribute excluded form saved
     * @return
     */
    public UpdateOptions exclude(DDataAttribute attribute) {
        excluded.add(attribute);
        return this;
    }


    private void extractorOfInterfaces(Class clazz, Set set) {
        if (clazz.getSuperclass() != null) {
            set.add(clazz.getSuperclass());
            extractorOfInterfaces(clazz.getSuperclass(), set);
        }
        for (Class anInterface : clazz.getInterfaces()) {
            set.add(anInterface);
            extractorOfInterfaces(anInterface, set);
        }
    }

    private Set<Class> extractionAllInterfaces(Class clazz) {
        HashSet interfacesOfObject = new LinkedHashSet();
        interfacesOfObject.add(clazz);
        extractorOfInterfaces(clazz, interfacesOfObject);
        return interfacesOfObject;
    }

    /**
     * registers the class and method of processing the bean before saving
     *
     * @param clazz   class for which the function of processing is registered
     * @param handler function of processing
     */
    public void register(Class clazz, Consumer handler) {
        beanHandlers.put(clazz, handler);
    }

    /**
     * will be called before saving a bean whose class or interface was added by the register method
     *
     * @param object handled bean
     */
    public void handledBean(Object object) {
        if (beanHandlers.isEmpty())
            return;
        extractionAllInterfaces(object.getClass()).forEach( s ->{
            if (beanHandlers.containsKey(s))
                beanHandlers.get(s).accept(object);
                });
    }

    /**
     * @param attributes   map of all classes of beans and their attributes that are contained in the stored bean,
     *                     including the bin itself
     * @param jsonIgnore   map of classes of beans and their attributes that are marked with JsonIgnore annotation
     * @param xmlTransient map of classes of beans and their attributes that are marked with XmlTransient annotation
     * @return map only incleded classes and attributes
     */
    public Set<DDataAttribute> generatedAttributes(
            Set<DDataAttribute> attributes,
            Set<DDataAttribute> jsonIgnore,
            Set<DDataAttribute> xmlTransient
    ) {
        Set<DDataAttribute> preparedAttributes = new HashSet<>(attributes);
        if (!excluded.isEmpty())
            preparedAttributes = preparedAttributes.stream().filter(s -> !excluded.contains(s)).collect(Collectors.toSet());

        if (!isIncludeJsonProps && !jsonIgnore.isEmpty())
            preparedAttributes = preparedAttributes.stream().filter(s -> !jsonIgnore.contains(s)).collect(Collectors.toSet());

        if (!isIncludeXmlProps && !xmlTransient.isEmpty())
            preparedAttributes = preparedAttributes.stream().filter(s -> !xmlTransient.contains(s)).collect(Collectors.toSet());

        return preparedAttributes;
    }


}
