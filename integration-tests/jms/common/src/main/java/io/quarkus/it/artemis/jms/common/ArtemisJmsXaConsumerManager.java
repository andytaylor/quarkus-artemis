package io.quarkus.it.artemis.jms.common;

import jakarta.jms.*;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

public class ArtemisJmsXaConsumerManager extends ArtemisJmsConsumerManager {
    private final XAConnectionFactory xaConnectionFactory;
    private final TransactionManager tm;
    private String queueName;

    /**
     * This constructor exists solely for CDI ("You need to manually add a non-private no-args constructor").
     */
    @SuppressWarnings("unused")
    ArtemisJmsXaConsumerManager() {
        this(null, null, null, null);
    }

    public ArtemisJmsXaConsumerManager(ConnectionFactory connectionFactory,
            XAConnectionFactory xaConnectionFactory,
            TransactionManager tm,
            String queueName) {
        super(connectionFactory, queueName);
        this.tm = tm;
        this.queueName = queueName;
        this.xaConnectionFactory = xaConnectionFactory;
    }

    public String receiveXA(boolean rollback) throws SystemException, RollbackException {
        XAJMSContext context = xaConnectionFactory.createXAContext();

        tm.getTransaction().enlistResource(context.getXAResource());
        tm.getTransaction().registerSynchronization(new Synchronization() {
            @Override
            public void beforeCompletion() {
            }

            @Override
            public void afterCompletion(int i) {
                context.close();
            }
        });
        JMSConsumer consumer = context.createConsumer(context.createQueue(queueName));
        if (rollback) {
            tm.setRollbackOnly();
        }
        try {
            return consumer.receive(1000L).getBody(String.class);
        } catch (JMSException e) {
            throw new RuntimeException("Could not receive message", e);
        }
    }
}
