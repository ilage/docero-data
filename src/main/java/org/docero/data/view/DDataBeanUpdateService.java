package org.docero.data.view;

import org.docero.data.utils.DDataAttribute;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class DDataBeanUpdateService<T> {
    private final Class<T> beanInterface;

    protected DDataBeanUpdateService(Class<T> beanInterface) {
        this.beanInterface = beanInterface;
    }

    boolean update(DDataViewRow row, Integer index, String entityPath) throws Exception {
        T bean = createBean();
        AbstractDataView.TableEntity entity = row.view.getEntityForPath(entityPath);
        updateBeanByRow(row, entity, bean, index, entityPath);

        // update properties used for mapping from parent entity if exists
        for (AbstractDataView.TableCell parentMapColumn : entity.mappings.keySet()) {
            AbstractDataView.TableCell beanMapColumn = entity.mappings.get(parentMapColumn);
            Object parentMapValue = row.getColumnValue(entity.isCollection() ? 0 : index, parentMapColumn.name);
            setProperty(bean, beanMapColumn.attribute, parentMapValue);
        }
        // call update services for children
        boolean anyChildUpdated = false;
        for (DDataAttribute attribute : entity.attributes)
            if (attribute.isMappedBean()) {
                DDataBeanUpdateService iu = row.view.getUpdateServiceFor(attribute.getBeanInterface());
                if (iu != null) {
                    iu.update(row, index, entityPath + "." + attribute.getPropertyName());
                    anyChildUpdated = true;
                }
            }
        if (anyChildUpdated) updateBeanByRow(row, entity, bean, index, entityPath);

        bean = updateBean(bean);

        // update parent properties used for mapping from bean
        boolean anyParentItemMayBeModified = false;
        if (entity.parent != null)
            for (AbstractDataView.TableCell parentMapColumn : entity.parent.mappings.keySet()) {
                AbstractDataView.TableCell beanMapColumn = entity.parent.mappings.get(parentMapColumn);
                Object beanMapValue = getProperty(bean, beanMapColumn.attribute);
                if (!DDataView.idIsNull(beanMapValue)) {
                    row.setColumnValue(beanMapValue, entity.parent.isCollection() ? 0 : index,
                            parentMapColumn.name, false);
                    anyParentItemMayBeModified = true;
                }
            }
        // write bean properties to view row if updates to database must be wrote by view
        if (this.serviceDoesNotMakeUpdates())
            for (DDataAttribute attribute : entity.attributes) {
                String property = attribute.getPropertyName();
                if (!attribute.isMappedBean() && property != null) {
                    Object val = getProperty(bean, attribute);
                    if (val != null)
                        row.setColumnValue(val, index, entityPath.length() == 0 ?
                                        property : entityPath + "." + property,
                                false);
                }
            }

        return anyParentItemMayBeModified;
    }

    private void updateBeanByRow(
            DDataViewRow row, AbstractDataView.TableEntity entity, T bean, Integer index, String entityPath
    ) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        for (DDataAttribute attribute : entity.attributes) {
            String property = attribute.getPropertyName();
            if (!attribute.isMappedBean() && property != null) {
                Object val = row.getColumnValue(index, entityPath.length() == 0 ? property :
                        entityPath + "." + property);
                if (val != null) setProperty(bean, attribute, val);
            }
        }
    }

    /*private DDataAttribute getParentAttribute(DDataViewRow row, String entityPath, int i, String parentColumnName) {
            return row.view.getEntityForPath(entityPath).attributes.stream()
                    .filter(a -> parentColumnName.equals(a.getColumnName()))
                    .findAny().orElse(null);
    }*/

    private Object getProperty(T bean, DDataAttribute attribute) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Class<?> beanImplementation = bean.getClass();
        String property = attribute.getPropertyName();
        String getterName = "get" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
        Method getter = beanImplementation.getMethod(getterName);
        return getter.invoke(bean);
    }

    private void setProperty(T bean, DDataAttribute attribute, Object val) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Class<?> beanImplementation = bean.getClass();
        String property = attribute.getPropertyName();
        Method setter = null;
        String setterName = "set" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
        if (attribute.getJavaType().isArray()) {
            setter = beanImplementation.getMethod(setterName, attribute.getJavaType());
            setter.invoke(bean, val);
        } else try {
            setter = beanImplementation.getMethod(setterName, attribute.getJavaType());
            setter.invoke(bean, val);
        } catch (NoSuchMethodException e) {
            if (Number.class.isAssignableFrom(attribute.getJavaType())) {
                if (Integer.class.isAssignableFrom(attribute.getJavaType())) {
                    setter = beanImplementation.getMethod(setterName, Integer.TYPE);
                    setter.invoke(bean, val == null ? 0 : ((Number) val).intValue());
                } else if (Long.class.isAssignableFrom(attribute.getJavaType())) {
                    setter = beanImplementation.getMethod(setterName, Long.TYPE);
                    setter.invoke(bean, val == null ? 0 : ((Number) val).longValue());
                } else if (Short.class.isAssignableFrom(attribute.getJavaType())) {
                    setter = beanImplementation.getMethod(setterName, Short.TYPE);
                    setter.invoke(bean, val == null ? 0 : ((Number) val).shortValue());
                } else if (Byte.class.isAssignableFrom(attribute.getJavaType())) {
                    setter = beanImplementation.getMethod(setterName, Byte.TYPE);
                    setter.invoke(bean, val == null ? 0 : ((Number) val).byteValue());
                } else if (Float.class.isAssignableFrom(attribute.getJavaType())) {
                    setter = beanImplementation.getMethod(setterName, Float.TYPE);
                    setter.invoke(bean, val == null ? 0 : ((Number) val).floatValue());
                } else if (Double.class.isAssignableFrom(attribute.getJavaType())) {
                    setter = beanImplementation.getMethod(setterName, Double.TYPE);
                    setter.invoke(bean, val == null ? 0 : ((Number) val).doubleValue());
                }
            } else if (Boolean.class.isAssignableFrom(attribute.getJavaType())) {
                setter = beanImplementation.getMethod(setterName, Boolean.TYPE);
                setter.invoke(bean, (Boolean) val);
            } else if (Character.class.isAssignableFrom(attribute.getJavaType())) {
                setter = beanImplementation.getMethod(setterName, Character.TYPE);
                setter.invoke(bean, (Character) val);
            }
        }
        if (setter == null) throw new NoSuchMethodException(beanInterface.getName() + "." + setterName);
    }

    protected abstract T createBean();

    protected abstract T updateBean(T bean) throws Exception;

    /**
     * If service does not make updates in database, then view try to update or insert row by known columns
     * (presented in view, and primary keys)
     *
     * @return default false
     */
    public boolean serviceDoesNotMakeUpdates() {
        return false;
    }
}
