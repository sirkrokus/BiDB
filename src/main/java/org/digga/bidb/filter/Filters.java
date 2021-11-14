package org.digga.bidb.filter;

public class Filters {

    public static EqualsFilter eq(String fieldName, String value) {
        return new EqualsFilter(fieldName, value);
    }

    public static NotEqualsFilter ne(String fieldName, String value) {
        return new NotEqualsFilter(fieldName, value);
    }

    public static AndFilter and() {
        return new AndFilter();
    }

    public static AndFilter and(Filter... filters) {
        return new AndFilter(filters);
    }

    public static OrFilter or(Filter... filters) {
        return new OrFilter(filters);
    }

    /*
    // сравнивает по AND
    public boolean match(Document document) {
        for (Filter filter : query.values()) {
            if (!filter.match(document)) {
                return false;
            }
        }
        return true;
    }

    // рекурсивно проходит через все поля в фильтре
    public void forEach(Consumer<FieldFilter> action) {
        for (String fieldName : query.keySet()) {
            Filter f = query.get(fieldName);
            if (f instanceof FieldFilter) {
                action.accept((FieldFilter)f);
            } else if (f instanceof BaseFilterList) {
                ((BaseFilterList)f).forEach(action);
            }
        }
    }
    */

}
