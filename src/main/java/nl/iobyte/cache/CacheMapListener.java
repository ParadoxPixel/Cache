package nl.iobyte.cache;

public interface CacheMapListener<T,R> {

    /**
     * Called when value is added
     * @param key T
     * @param value R
     */
    void onAdd(T key, R value);

    /**
     * Called when value is removed
     * @param key T
     * @param value R
     */
    void onRemoval(T key, R value);

}
