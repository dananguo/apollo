package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DatabaseMessageSender implements MessageSender {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);

    /**
     * 清理 Message 队列 最大容量
     */
    private static final int CLEAN_QUEUE_MAX_SIZE = 100;
    /**
     * 清理 Message ExecutorService
     */
    private final ExecutorService cleanExecutorService;
    /**
     * 是否停止清理 Message 标识
     */
    private final AtomicBoolean cleanStopped;
    private final ReleaseMessageRepository releaseMessageRepository;
    /**
     * 清理 Message 队列
     */
    private BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);

    public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
        cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
        cleanStopped = new AtomicBoolean(false);
        this.releaseMessageRepository = releaseMessageRepository;
    }

    @Override
    @Transactional
    public void sendMessage(String message, String channel) {
        logger.info("Sending message {} to channel {}", message, channel);
        if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {
            logger.warn("Channel {} not supported by DatabaseMessageSender!", channel);
            return;
        }

        Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
        Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
        try {
            ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
            toClean.offer(newMessage.getId());
            transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
            logger.error("Sending message to database failed", ex);
            transaction.setStatus(ex);
            throw ex;
        } finally {
            transaction.complete();
        }
    }

    @PostConstruct
    private void initialize() {
        cleanExecutorService.submit(() -> {
            while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    Long rm = toClean.poll(1, TimeUnit.SECONDS);
                    if (rm != null) {
                        cleanMessage(rm);
                    } else {
                        TimeUnit.SECONDS.sleep(5);
                    }
                } catch (Throwable ex) {
                    Tracer.logError(ex);
                }
            }
        });
    }

    /**
     * 清理老消息们，调用 {@link ReleaseMessageRepository#findOne(id)} 方法，查询对应的 {@link ReleaseMessage} 对象，避免已经删除。
     * <p>
     * 因为，{@link DatabaseMessageSender} 会在多进程中执行。例如：1）Config Service + Admin Service ；2）N Config Service ；3）N Admin Service 。
     * 为什么 Config Service 和 Admin Service 都会启动清理任务呢？😈 因为 {@link DatabaseMessageSender} 添加了 @Component 注解，
     * 而 {@link com.ctrip.framework.apollo.biz.service.NamespaceService} 注入了 DatabaseMessageSender 。
     * 而 {@link com.ctrip.framework.apollo.biz.service.NamespaceService} 被 apollo-adminservice 和 apoll-configservice 项目都引用了，所以都会启动该任务。
     *
     * @param id
     */
    private void cleanMessage(Long id) {
        //double check in case the release message is rolled back
        ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
        if (releaseMessage == null) {
            return;
        }
        boolean hasMore = true;
        while (hasMore && !Thread.currentThread().isInterrupted()) {
            List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
                    releaseMessage.getMessage(), releaseMessage.getId());

            releaseMessageRepository.deleteAll(messages);
            hasMore = messages.size() == 100;

            messages.forEach(toRemove -> Tracer.logEvent(
                    String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
        }
    }

    void stopClean() {
        cleanStopped.set(true);
    }
}
