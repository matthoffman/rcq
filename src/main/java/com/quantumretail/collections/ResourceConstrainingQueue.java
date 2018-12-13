package com.quantumretail.collections;

import com.quantumretail.metrics.MetricsAware;
import com.quantumretail.constraint.ConstraintStrategies;
import com.quantumretail.constraint.ConstraintStrategy;
import com.quantumretail.rcq.predictor.TaskTracker;
import com.quantumretail.rcq.predictor.TaskTrackers;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This class is the parent class for the various RCQ implementations.
 * The implementations have different tradeoffs in regards to performance and behavior in the event of multiple consumers.
 * In general, users should be able to use either the builder located in this class, or the static helper methods
 * ({@link ResourceConstrainingQueues}) to construct the appropriate subclass, without worrying too much about the
 * details. But see the javadoc for the subclasses for more details on the tradeoffs they make.
 * <p>
 * Note that this resource-constraining behavior ONLY occurs on {@link #poll()}, {@link #take()} and {@link #remove()}.
 * Other access methods like {@link #peek()}, {@link #iterator()}, {@link #toArray()}, and so on will bypass the
 * resource-constraining behavior.
 * <p/>

 */
public abstract class ResourceConstrainingQueue<T> implements BlockingQueue<T>, MetricsAware {
    private static final Logger log = LoggerFactory.getLogger(ResourceConstrainingQueue.class);

    public static <T> ResourceConstrainingQueueBuilder<T> builder() {
        return new ResourceConstrainingQueueBuilder<>();
    }

    private boolean failAfterAttemptThresholdReached = false;

    public static final long DEFAULT_POLL_FREQ_MS = 100L;
    //the default will try for 10 mins  (default poll freq = 100L)
    protected static final long DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS = (10 * 60 * 1000) / DEFAULT_POLL_FREQ_MS;

    final BlockingQueue<T> delegate;
    long retryFrequencyMS = DEFAULT_POLL_FREQ_MS;
    long constrainedItemThresholdMS = DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS;

    final ConstraintStrategy<T> constraintStrategy;

    final TaskTracker<T> taskTracker;
    final TaskAttemptCounter taskAttemptCounter;

    private Meter trackedRemovals = null;
    private Meter additions = null;
    private Counter pendingItems = null;
    private Meter sleeps = null;


    // this is the lock we'll use if buffer == true or strict == true.
    ReentrantLock takeLock = new ReentrantLock(true);

    /**
     * Use the static {@link #builder()} method to construct a ResourceConstrainingQueue
     */
    protected ResourceConstrainingQueue(BlockingQueue<T> delegate, ConstraintStrategy<T> constraintStrategy, long retryFrequencyMS, TaskTracker<T> taskTracker, long constrainedItemThresholdMS) {

        this.delegate = delegate;
        this.retryFrequencyMS = retryFrequencyMS;
        this.constraintStrategy = constraintStrategy;
        this.taskTracker = taskTracker;
        this.constrainedItemThresholdMS = constrainedItemThresholdMS;
        this.taskAttemptCounter = new TaskAttemptCounter();
    }

    protected T trackIfNecessary(T item) {
        // if item == null, nothing to track.
        if (item == null) return null;

        if (trackedRemovals != null) {
            trackedRemovals.mark();
        }
        if (pendingItems != null) {
            pendingItems.dec();
        }
        if (taskAttemptCounter != null) {
            taskAttemptCounter.removeConstrained(item);
        }
        if (taskTracker != null) {
            return taskTracker.register(item);
        } else {
            return item;
        }
    }

    public boolean add(T t) {
        markAddition();
        return delegate.add(t);
    }

    private void markAddition() {
        if (pendingItems != null) {
            pendingItems.inc();
        }
        if (additions != null) {
            additions.mark();
        }
    }

    public boolean offer(T t) {
        markAddition();
        return delegate.offer(t);
    }

    /**
     * Retrieves and removes the head of this queue.  This method differs
     * from {@link #poll poll} only in that it throws an exception if this
     * queue is empty.
     * <p>
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     *
     * @return the head of this queue
     * @throws NoSuchElementException         if this queue is empty
     * @throws InsufficientResourcesException if we do not have sufficient resources for the next element
     */
    @Override
    public T remove() {
        return getItemNonBlocking(() -> {
            throw new NoSuchElementException();
        }, (n) -> {
            throw new InsufficientResourcesException(n);
        });
    }

    /**
     * Get an item if it is available, or the value of onNoElementAvailable if there is nothing available, or onInsufficientResources
     * if there is an item available but there are insufficient resources.
     *
     * @param onNoElementAvailable    what to return when there's nothing available
     * @param onInsufficientResources what to return when there's something available, but we don't have sufficient
     *                                resources to execute it.
     * @return an item if present, or the value returned by the appropriate input function/supplier otherwise
     */
    protected abstract T getItemNonBlocking(Supplier<T> onNoElementAvailable, Function<T, T> onInsufficientResources);

    /**
     * Get an item, blocking as necessary until one is available and ready to execute. If timeout >= 0, we will wait at
     * most timeout (on a best-effort basis). If timeout < 0, this method blocks indefinitely.
     *
     * @param timeout if < 0, it is treated as "no timeout". Otherwise, treated as a best-effort max wait.
     * @return an item if present, or the value returned by the appropriate input function/supplier otherwise
     */
    protected abstract T getItemBlocking(long timeout, TimeUnit unit) throws InterruptedException;


    protected boolean shouldReturn(T nextItem) {

        boolean shouldReturn = constraintStrategy.shouldReturn(nextItem);
        if (!shouldReturn && (taskTracker != null && taskTracker.currentTasks().isEmpty())) {
            if (log.isDebugEnabled()) {
                log.debug("Constraint strategy says we should not return an item, but task tracker says that there is nothing in progress. So returning it anyway.");
            }
            return true;
        } else {
            return shouldReturn;
        }
    }


    /**
     * Retrieves and removes the head of this queue,
     * or returns {@code null} if this queue is empty, or if we do not yet have sufficient resources for the next item.
     * <p>
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     *
     * @return the next value in the queue or null if we cannot currently execute anything.
     */
    @Override
    public T poll() {
        return getItemNonBlocking(() -> null, (n) -> null);
    }


    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     * <p>
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     * <p>
     * Also note that, since this implementation depends on peek(), it is implemented using a periodic poll of the
     * underlying queue, rather than calling queue.take() directly. The poll frequency can be set as a constructor argument.
     * TODO: exponential decay on the poll frequency
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public T take() throws InterruptedException {
        return poll(-1, TimeUnit.NANOSECONDS);
    }

    T failForTooMayTries(T item) {
        log.error("Could not take item after " + constrainedItemThresholdMS + " attempts:  " + item);
        //take the item from the delegate
        delegate.remove();
        taskAttemptCounter.removeConstrained(item);
        return failTask(item);
    }


    /**
     * If you would like to implement custom logic after an item has failed too many resource checks, override this method.
     * Some options are to call "cancel()" on tasks, register exceptions, requeue, etc.
     * <p>
     * Note that the result of this method will be returned to the original caller.
     *
     * @param item
     * @return
     */
    protected T failTask(T item) {
        throw new InsufficientResourcesException(item);
    }

    /**
     * If we decide we want pluggable behavior here, take a look at LMAX Disruptor's WaitStrategy classes
     */
    protected void sleep() throws InterruptedException {
        if (sleeps != null) {
            sleeps.mark();
        }
        Thread.sleep(retryFrequencyMS);
    }

    /**
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     * <p>
     * Also note that, since this implementation depends on peek(), it is implemented using a periodic poll of the
     * underlying queue, rather than calling queue.take() directly. The poll frequency can be set as a constructor argument.
     *
     * @return the head of this queue, or {@code null} if the
     * specified waiting time elapses before an element which we have the resources for is available
     * @see #poll()
     */
    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return getItemBlocking(timeout, unit);
    }

    protected boolean shouldFail(T nextItem) {
        if (taskAttemptCounter != null) {
            //increment number of tries for this item
            int attempts = taskAttemptCounter.incrementConstrained(nextItem);
            if (attempts >= constrainedItemThresholdMS) {
                if (failAfterAttemptThresholdReached) {
                    return true;
                } else {
                    //just log it and continue to try
                    if (log.isTraceEnabled()) {
                        log.trace("Could not take item after " + (constrainedItemThresholdMS * retryFrequencyMS / 1000.0) + " seconds:" + nextItem);
                    }
                    //set retries back to 1
                    taskAttemptCounter.resetConstrained(nextItem);
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#element()
     */
    @Override
    public T element() {
        return delegate.element();
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#peek()
     */
    @Override
    public T peek() {
        return delegate.peek();
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue. So it is
     * literally "how many elements in the queue?" not "how many elements do I have resources to execute?"
     *
     * @see java.util.Queue#size()
     */
    @Override
    public int size() {
        return delegate.size();
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue. So it is
     * literally "are there any elements in the queue?" not "do I have resources to execute any elements in the queue?"
     *
     * @see java.util.Queue#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#contains(Object)
     */
    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection)
     */
    public int drainTo(Collection<? super T> c) {
        int count = delegate.drainTo(c);
        if (pendingItems != null) {
            pendingItems.dec(count);
        }
        return count;
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection, int)
     */
    public int drainTo(Collection<? super T> c, int maxElements) {
        int count = delegate.drainTo(c, maxElements);
        if (pendingItems != null) {
            pendingItems.dec(count);
        }
        return count;
    }

    /**
     * This method just delegates to the underlying queue; the returned iterator will NOT honor any resource constraints.
     * This may change in the future.
     *
     * @see java.util.concurrent.BlockingQueue#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#toArray()
     */
    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#toArray(Object[])
     */
    @Override
    public <T1> T1[] toArray(T1[] a) {
        return delegate.toArray(a);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#remove(Object)
     */
    @Override
    public boolean remove(Object o) {
        if (pendingItems != null) {
            pendingItems.dec();
        }
        return delegate.remove(o);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#containsAll(java.util.Collection)
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#addAll(java.util.Collection)
     */
    public boolean addAll(Collection<? extends T> c) {
        if (pendingItems != null) {
            pendingItems.inc(c.size());
        }
        return delegate.addAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#removeAll(java.util.Collection)
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        if (pendingItems != null) {
            pendingItems.dec(c.size());
        }
        return delegate.removeAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#retainAll(java.util.Collection)
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        //TODO: pendingItems isn't tracking changes via this method yet.
        return delegate.retainAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#clear()
     */
    @Override
    public void clear() {
        if (pendingItems != null) {
            int size = delegate.size();
            pendingItems.dec(size);
        }
        delegate.clear();
    }

    /**
     * returns true if o is an instance of ResourceConstrainingQueues and their underlying queues are equal.
     * Most of the definition of "equality", then, is delegated to the underlying queues.
     *
     * @see java.util.Collection#equals(Object)
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof ResourceConstrainingQueue && delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }


    /**
     * Add t to the back of the queue. Note that ResourceConstrainingQueue doesn't make any resource constraint checks
     * on insertions into the queue, only on removals.
     *
     * @param t
     * @throws InterruptedException
     * @see java.util.concurrent.BlockingQueue#put(Object)
     */
    public void put(T t) throws InterruptedException {
        markAddition();
        delegate.put(t);
    }

    /**
     * Note that ResourceConstrainingQueue doesn't make any resource constraint checks
     * on insertions into the queue, only on removals.
     *
     * @param t
     * @throws InterruptedException
     * @see java.util.concurrent.BlockingQueue#offer(Object, long, java.util.concurrent.TimeUnit)
     */
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        markAddition();
        return delegate.offer(t, timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    /**
     * Get the constraint strategy that we're using to decide whether to hand out items.
     *
     * @return
     */
    public ConstraintStrategy<T> getConstraintStrategy() {
        return constraintStrategy;
    }

    public void registerMetrics(MetricRegistry metrics, String name) {
        metrics.register(name(ResourceConstrainingQueue.class, name, "size"), (Gauge<Integer>) this::size);


        pendingItems = metrics.counter(name(ResourceConstrainingQueue.class, name, "pending-items"));
        trackedRemovals = metrics.meter(name(ResourceConstrainingQueue.class, name, "remove-poll-take"));
        additions = metrics.meter(name(ResourceConstrainingQueue.class, name, "add-offer-put"));
        sleeps = metrics.meter(name(ResourceConstrainingQueue.class, "sleeps"));

        if (this.constraintStrategy instanceof MetricsAware) {
            ((MetricsAware) constraintStrategy).registerMetrics(metrics, name);
        }
        if (this.delegate instanceof MetricsAware) {
            ((MetricsAware) delegate).registerMetrics(metrics, name);
        }
        if (this.taskTracker instanceof MetricsAware) {
            ((MetricsAware) taskTracker).registerMetrics(metrics, name);
        }

    }

    public boolean isFailAfterAttemptThresholdReached() {
        return failAfterAttemptThresholdReached;
    }

    public void setFailAfterAttemptThresholdReached(boolean failAfterAttemptThresholdReached) {
        this.failAfterAttemptThresholdReached = failAfterAttemptThresholdReached;
    }


    public static class ResourceConstrainingQueueBuilder<T> {
        BlockingQueue<T> builderdelegate = null;
        private long builderresourcePollFrequencyMS = DEFAULT_POLL_FREQ_MS;
        ConstraintStrategy<T> builderConstraintStrategy;
        TaskTracker<T> builderTaskTracker;
        boolean useTaskTracker = true;
        boolean builderStrict = true;
        boolean useBuffer = true;
        long noResourcesTimeLimit = DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS;

        public ResourceConstrainingQueueBuilder<T> withConstraintStrategy(ConstraintStrategy<T> cs) {
            this.builderConstraintStrategy = cs;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> withTaskTracker(TaskTracker<T> tt) {
            this.builderTaskTracker = tt;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> useTaskTracker(boolean useTaskTracker) {
            this.useTaskTracker = useTaskTracker;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> withBlockingQueue(BlockingQueue<T> q) {
            this.builderdelegate = q;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> useBuffer(boolean buffer) {
            this.useBuffer = buffer;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> withRetryFrequency(long pollFrequencyInMS) {
            this.builderresourcePollFrequencyMS = pollFrequencyInMS;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> strict(boolean strict) {
            this.builderStrict = strict;
            return this;
        }

        /**
         * The maximum amount of time to wait until there are sufficient resources for an item.
         *
         * @param timeLimit
         * @param timeUnit
         * @return
         */
        public ResourceConstrainingQueueBuilder<T> withConstrainedItemTimeLimit(long timeLimit, TimeUnit timeUnit) {
            this.noResourcesTimeLimit = timeUnit.toMillis(timeLimit);
            return this;
        }

        public ResourceConstrainingQueue<T> build() {
            BlockingQueue<T> d = builderdelegate;
            long pollfreq = builderresourcePollFrequencyMS;
            ConstraintStrategy<T> cs = builderConstraintStrategy;
            if (cs == null) {
                cs = ConstraintStrategies.defaultReactiveConstraintStrategy(pollfreq);
            }
            if (d == null) {
                d = new LinkedBlockingQueue<T>();
            }
            if (useTaskTracker && builderTaskTracker == null) {
                builderTaskTracker = TaskTrackers.defaultTaskTracker();
            }
            if (this.useBuffer) {
                return new BufferingResourceConstrainingQueue<>(d, cs, pollfreq, builderTaskTracker, noResourcesTimeLimit);
            } else {
                return new PeekingResourceConstrainingQueue<>(d, cs, pollfreq, builderStrict, builderTaskTracker, noResourcesTimeLimit);
            }
        }

    }


    protected static class TaskAttemptCounter {

        final ConcurrentHashMap<Object, Integer> unableToExecuteTaskTries = new ConcurrentHashMap<Object, Integer>();

        public int incrementConstrained(Object item) {
            Integer attempts = unableToExecuteTaskTries.get(item);
            if (attempts == null) {
                attempts = 0;
            }
            attempts = attempts + 1;
            unableToExecuteTaskTries.put(item, attempts);
            return attempts;
        }

        public void resetConstrained(Object item) {
            if (unableToExecuteTaskTries.contains(item)) {
                unableToExecuteTaskTries.put(item, 1);
            }
        }

        public void removeConstrained(Object item) {
            unableToExecuteTaskTries.remove(item);
        }

    }

    public static class InsufficientResourcesException extends NoSuchElementException {
        public InsufficientResourcesException(Object o) {
            super("Insufficient resources to execute " + o.toString());
        }

        // sometimes you feel like an arg... sometimes you don't.
        public InsufficientResourcesException() {
        }
    }
}
