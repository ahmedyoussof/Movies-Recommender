package com.recommendation.data;

public interface AbstractDAO<T, ID> {
    
    /**
     * finds element by id
     * @param id refers to element id
     * @return element of type T
     */
    public T findById(ID id);

    /**
     * inserts row into db
     * @param entity identifies db row
     * @return true if inserted
     */
    public boolean insert(T entity);
    
    /**
     * updates db row
     * @param entity identifies db row
     * @return true if updated
     */
    public boolean update(T entity);

    /**
     * delete db row
     * @param id identifies db row
     * @return ture if updated
     */
    public boolean delete(ID id);
}
