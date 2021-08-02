package nl.iobyte.cache;

import nl.iobyte.dataapi.bucket.map.interfaces.partition.IMapBucketPartition;
import nl.iobyte.dataapi.bucket.map.interfaces.partition.IMapPartitionStrategy;
import nl.iobyte.dataapi.bucket.map.objects.AbstractMapBucket;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CacheMap<T,R> extends AbstractMapBucket<T,R> {

    private final long expiryInMillis;
    private CacheMapListener<T,R> listener;
    private final boolean update;

    private final Map<T, Long> timeMap = new ConcurrentHashMap<>();
    private final AtomicBoolean mapAlive = new AtomicBoolean(true);

    public CacheMap(int partitions, IMapPartitionStrategy<T,R> strategy, long expiryInMillis) {
        this(partitions, strategy, expiryInMillis, false);
    }

    public CacheMap(int partitions, IMapPartitionStrategy<T,R> strategy, long expiryInMillis, boolean update) {
        super(partitions, strategy);
        this.expiryInMillis = expiryInMillis;
        this.update = update;
    }

    public CacheMap(int partitions, IMapPartitionStrategy<T,R> strategy, long expiryInMillis, CacheMapListener<T,R> listener) {
        this(partitions, strategy, expiryInMillis, listener, false);
    }

    public CacheMap(int partitions, IMapPartitionStrategy<T,R> strategy, long expiryInMillis, CacheMapListener<T,R> listener, boolean update) {
        super(partitions, strategy);
        this.expiryInMillis = expiryInMillis;
        this.listener = listener;
        this.update = update;
    }

    /**
     * Start cleaner thread
     */
    void initialize() {
        new CleanerThread().start();
    }

    /**
     * {@inheritDoc}
     * @return Map<T,R>
     */
    public Map<T, R> createMap() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Set listener for map
     * @param listener CacheMapListener<T,R>
     */
    public void setListener(CacheMapListener<T, R> listener) {
        this.listener = listener;
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if trying to insert values into map after quiting
     */
    @Override
    public R put(T key, R value) {
        if (!mapAlive.get())
            throw new IllegalStateException("WeakConcurrent Hashmap is no more alive.. Try creating a new one.");	// No I18N

        Date date = new Date();
        timeMap.put(key, date.getTime());
        R returnVal = super.put(key, value);
        if (listener != null)
            listener.onAdd(key, value);

        return returnVal;
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if trying to insert values into map after quiting
     */
    @Override
    public void putAll(Map<? extends T, ? extends R> m) {
        if (!mapAlive.get())
            throw new IllegalStateException("WeakConcurrent Hashmap is no more alive.. Try creating a new one.");	// No I18N

        for (T key : m.keySet())
            put(key, m.get(key));
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if trying to insert values into map after quiting
     */
    @Override
    public R putIfAbsent(T key, R value) {
        if (!mapAlive.get())
            throw new IllegalStateException("WeakConcurrent Hashmap is no more alive.. Try creating a new one.");	// No I18N

        if (!containsKey(key)) {
            return put(key, value);
        } else {
            return get(key);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public R get(Object key) {
        if (!mapAlive.get())
            throw new IllegalStateException("WeakConcurrent Hashmap is no more alive.. Try creating a new one.");	// No I18N

        R value = super.get(key);
        if(value == null)
            return null;

        if(update) {
            Date date = new Date();
            timeMap.put((T) key, date.getTime());
        }

        return value;
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if trying to insert values into map after quiting
     */
    @Override
    @SuppressWarnings("unchecked")
    public R remove(Object key) {
        timeMap.remove(key);
        R value = super.remove(key);
        if(value == null)
            return null;

        if(listener != null)
            listener.onRemoval((T) key, value);

        return value;
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if trying to insert values into map after quiting
     */
    @Override
    public R computeIfAbsent(T key, Function<? super T, ? extends R> f) {
        if (!mapAlive.get())
            throw new IllegalStateException("WeakConcurrent Hashmap is no more alive.. Try creating a new one.");	// No I18N

        if (!containsKey(key)) {
            R value = f.apply(key);
            put(key, value);
            return value;
        } else {
            return get(key);
        }
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if trying to insert values into map after quiting
     */
    @Override
    public R computeIfPresent(T key, BiFunction<? super T, ? super R, ? extends R> f) {
        if (!mapAlive.get())
            throw new IllegalStateException("WeakConcurrent Hashmap is no more alive.. Try creating a new one.");	// No I18N

        if (!containsKey(key)) {
            return null;
        } else {
            R old = get(key);
            R value = f.apply(key, old);
            if(value == null)
                return remove(key);

            put(key, value);
            return old;
        }
    }

    /**
     * Should call this method when map is no longer required
     */
    public void stop() {
        mapAlive.set(true);
    }

    /**
     * Get if map is alive
     * @return Boolean
     */
    public boolean isAlive() {
        return mapAlive.get();
    }

    /**
     * Call to clean next partition
     */
    public void cleanMap() {
        long currentTime = new Date().getTime();
        IMapBucketPartition<T,R> partition = CacheMap.this.asStepper().next();
        for(T key : partition.keySet()) {
            if (currentTime > (timeMap.get(key) + expiryInMillis)) {
                R value = super.remove(key);
                timeMap.remove(key);
                if (listener != null)
                    listener.onRemoval(key, value);
            }
        }
    }

    /**
     * Call to clean all partitions
     */
    public void cleanAll() {
        long currentTime = new Date().getTime();
        for(IMapBucketPartition<T,R> partition : getPartitions()) {
            for(T key : partition.keySet()) {
                if (currentTime > (timeMap.get(key) + expiryInMillis)) {
                    R value = remove(key);
                    timeMap.remove(key);
                    if (listener != null)
                        listener.onRemoval(key, value);
                }
            }
        }
    }

    /**
     * This thread performs the cleaning operation on the concurrent hashmap once in a specified interval. This wait interval is half of the
     * time from the expiry time.
     */
    class CleanerThread extends Thread {

        @Override
        public void run() {
            while (mapAlive.get()) {
                cleanMap();
                try {
                    Thread.sleep(expiryInMillis / getPartitionCount() / 2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
