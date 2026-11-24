/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.mail;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.Store;
import jakarta.mail.internet.MimeMessage;

import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.component.mail.Mailbox.MailboxUser;
import org.apache.camel.component.mail.Mailbox.Protocol;
import org.apache.camel.support.KeyValueIdempotentRepository;
import org.apache.camel.support.UnitOfWorkHelper;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * With pollEnrich or a ConsumerTemplate, the exchange of a mail can complete after its consumer was stopped. The stop
 * closed the folder and the store; the completion must not open the folder or connect again (IMAPStore would log in
 * with the credentials the closed store kept, and nobody would close that connection). The mail is not marked and its
 * idempotent key is settled as on commit. A poll that comes after the stop does not connect either.
 */
class MailConsumerCommitAfterStopTest extends CamelTestSupport {
    private static final MailboxUser bob = Mailbox.getOrCreateUser("MailConsumerCommitAfterStopTest-bob", "secret");

    @BindToRegistry("repo")
    private final KeyValueIdempotentRepository repo = new KeyValueIdempotentRepository();

    private final Queue<Exchange> queued = new ConcurrentLinkedQueue<>();

    @Override
    public void setupResources() throws Exception {
        Mailbox.clearAll(bob);
        JavaMailSender sender = new DefaultJavaMailSender();
        Store store = sender.getSession().getStore("imap");
        store.connect("localhost", Mailbox.getPort(Protocol.imap), bob.getLogin(), bob.getPassword());
        Folder folder = store.getFolder("INBOX");
        folder.open(Folder.READ_WRITE);
        Message message = new MimeMessage(sender.getSession());
        message.setSubject("Mail 0");
        message.setText("Message 0");
        folder.appendMessages(new Message[] { message });
        folder.close(true);
        store.close();
    }

    @Test
    void imapCommitAfterStopDoesNotConnectAgain() throws Exception {
        commitAfterStop(bob.uriPrefix(Protocol.imap) + "&idempotentRepository=#repo");
        assertEquals(1, bob.getInbox().getNewMessageCount(), "the mail is not marked SEEN");
    }

    @Test
    void imapCommitAfterStopWithCloseFolderDoesNotConnectAgain() throws Exception {
        commitAfterStop(bob.uriPrefix(Protocol.imap) + "&closeFolder=true&idempotentRepository=#repo");
        assertEquals(1, bob.getInbox().getNewMessageCount(), "the mail is not marked SEEN");
    }

    @Test
    void pop3CommitAfterStopDoesNotConnectAgain() throws Exception {
        commitAfterStop(bob.uriPrefix(Protocol.pop3) + "&delete=true&idempotentRepository=#repo");
        assertEquals(1, bob.getInbox().getMessageCount(), "the mail is not deleted");
    }

    @Test
    void pollAfterStopDoesNotConnectAgain() throws Exception {
        MailConsumer consumer = createConsumer(bob.uriPrefix(Protocol.imap) + "&idempotentRepository=#repo");
        consumer.stop();

        // a poll that was already running when the consumer stopped
        assertEquals(0, consumer.poll(), "a stopped consumer does not poll the mailbox");
        assertTrue(queued.isEmpty());
        assertEquals(1, bob.getInbox().getNewMessageCount());
    }

    private void commitAfterStop(String uri) throws Exception {
        MailConsumer consumer = createConsumer(uri);
        assertEquals(1, consumer.poll());
        Exchange exchange = queued.poll();
        assertNotNull(exchange);
        String key = exchange.getProperty(MailConsumer.MAIL_MESSAGE_UID, String.class);
        assertTrue(repo.contains(key));
        Folder folder = ((MailMessage) exchange.getIn()).getOriginalMessage().getFolder();

        consumer.stop();
        assertFalse(folder.isOpen());
        assertFalse(folder.getStore().isConnected());

        // the exchange completes now (ConsumerTemplate.doneUoW, or the end of pollEnrich)
        UnitOfWorkHelper.doneSynchronizations(exchange, exchange.getExchangeExtension().handoverCompletions());

        boolean open = folder.isOpen();
        try {
            assertFalse(open, "the stopped consumer did not open the folder again");
            assertFalse(folder.getStore().isConnected(), "the stopped consumer did not connect again");
            assertFalse(repo.contains(key), "the key of the mail that is not marked is removed, as on commit");
        } finally {
            if (open) {
                folder.close(false);
            }
        }
    }

    /**
     * A consumer whose processor only queues the exchange, as the polling consumer of pollEnrich and ConsumerTemplate
     * does: the exchange completes later, on the test thread.
     */
    private MailConsumer createConsumer(String uri) throws Exception {
        MailEndpoint endpoint = context.getEndpoint(uri, MailEndpoint.class);
        JavaMailSender sender = endpoint.getConfiguration().createJavaMailSender(context);
        endpoint.getConfiguration().configureJavaMailSender(context, sender);
        MailConsumer consumer = new MailConsumer(endpoint, queued::add, sender);
        consumer.setStartScheduler(false);
        consumer.start();
        return consumer;
    }
}
