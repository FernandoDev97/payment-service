import { Injectable, Logger } from '@nestjs/common';
import { PaymentOrderMessage } from '../payment-queue.interface';
import { RabbitmqService } from '../rabbitmq/rabbitmq.service';

export interface DLQMessage {
  content: PaymentOrderMessage;
  properties: {
    messageId?: string;
    timestamp?: number;
    headers?: Record<string, unknown>;
  };
  deathInfo?: {
    reason: string;
    queue: string;
    time: Date;
    count: number;
    exchange: string;
    routingKeys: string[];
  };
}

export interface DLQStats {
  queueName: string;
  messageCount: number;
  consumerCount: number;
}

@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);

  private readonly DLQ_NAME = 'payment_queue.dlq';
  private readonly EXCHANGE = 'payments';
  private readonly ROUTING_KEY = 'payment.order';

  constructor(private readonly rabbitmqService: RabbitmqService) {}

  /**
   * Obtém estatísticas da DLQ
   */

  async getStats(): Promise<DLQStats> {
    const channel = this.rabbitmqService.getChannel();
    if (!channel) {
      throw new Error('RabbitMQ channel not available');
    }

    const queueInfo = await channel.checkQueue(this.DLQ_NAME);

    return {
      queueName: this.DLQ_NAME,
      messageCount: queueInfo.messageCount,
      consumerCount: queueInfo.consumerCount,
    };
  }

  /**
   * Obtém mensagens da DLQ sem removê-las (peek)
   * Usa o conceito de "get" com nack para visualizar sem consumir
   */
  async peekMessages(limit: number = 10): Promise<DLQMessage[]> {
    const channel = this.rabbitmqService.getChannel();
    if (!channel) {
      throw new Error('RabbitMQ channel not available');
    }

    const messages: DLQMessage[] = [];

    await channel.checkQueue(this.DLQ_NAME);

    for (let i = 0; i < limit; i++) {
      const msg = await channel.get(this.DLQ_NAME, { noAck: false });

      if (!msg) {
        break; // Não há mais mensagens
      }

      try {
        const content = JSON.parse(
          msg.content.toString(),
        ) as PaymentOrderMessage;

        const xDeath = msg.properties.headers?.['x-death'] as
          | Array<{
              reason: string;
              queue: string;
              time: { getTime: () => number };
              count: number;
              exchange: string;
              'routing-keys': string[];
            }>
          | undefined;

        const deathInfo = xDeath?.[0]
          ? {
              reason: xDeath[0].reason,
              queue: xDeath[0].queue,
              time: new Date(xDeath[0].time?.getTime?.() || Date.now()),
              count: xDeath[0].count,
              exchange: xDeath[0].exchange,
              routingKeys: xDeath[0]['routing-keys'],
            }
          : undefined;
        const headers =
          msg.properties.headers && typeof msg.properties.headers === 'object'
            ? (msg.properties.headers as Record<string, unknown>)
            : undefined;
        messages.push({
          content,
          properties: {
            messageId: msg.properties.messageId as string | undefined,
            timestamp: msg.properties.timestamp as number | undefined,
            headers,
          },
          deathInfo,
        });
        channel.nack(msg, false, true);
      } catch (error) {
        channel.nack(msg, false, true);
        this.logger.error('Failed to parse DLQ message:', error);
      }
    }
    return messages;
  }

  /**
   * Reprocessa uma mensagem específica da DLQ
   * Remove da DLQ e publica na fila principal
   */

  async reprocessMessage(orderId: string): Promise<boolean> {
    const channel = this.rabbitmqService.getChannel();
    if (!channel) {
      throw new Error('RabbitMQ channel not available');
    }

    const stats = await this.getStats();
    let found = false;

    for (let i = 0; i < stats.messageCount; i++) {
      const msg = await channel.get(this.DLQ_NAME, { noAck: false });

      if (!msg) break;

      try {
        const content = JSON.parse(
          msg.content.toString(),
        ) as PaymentOrderMessage;

        if (content.orderId === orderId) {
          // Encontrou! Republica na fila principal
          await this.rabbitmqService.publishMessage(
            this.EXCHANGE,
            this.ROUTING_KEY,
            content,
          );

          // Remove da DLQ (ack)
          channel.ack(msg);
          found = true;

          this.logger.log(`✅ Message ${orderId} reprocessed successfully`);
          break;
        } else {
          // Não é a mensagem que procuramos, devolve para DLQ
          channel.nack(msg, false, true);
        }
      } catch (error) {
        channel.nack(msg, false, true);
        this.logger.error('Failed to process DLQ message:', error);
      }
    }
    return found;
  }

  /**
   * Reprocessa todas as mensagens da DLQ
   */
  async reprocessAll(): Promise<{ processed: number; failed: number }> {
    const channel = this.rabbitmqService.getChannel();
    if (!channel) {
      throw new Error('RabbitMQ channel not available');
    }

    const stats = await this.getStats();
    let processed = 0;
    let failed = 0;

    this.logger.log(`🔄 Reprocessing ${stats.messageCount} messages from DLQ`);

    for (let i = 0; i < stats.messageCount; i++) {
      const msg = await channel.get(this.DLQ_NAME, { noAck: false });

      if (!msg) break;

      try {
        const content = JSON.parse(
          msg.content.toString(),
        ) as PaymentOrderMessage;

        // Republica na fila principal
        await this.rabbitmqService.publishMessage(
          this.EXCHANGE,
          this.ROUTING_KEY,
          content,
        );
        // Remove da DLQ
        channel.ack(msg);
        processed++;

        this.logger.log(`✅ Reprocessed order ${content.orderId}`);
      } catch (error) {
        // Falhou, mantém na DLQ
        channel.nack(msg, false, true);
        failed++;
        this.logger.error('Failed to reprocess message:', error);
      }
    }

    this.logger.log(
      `🏁 Reprocess complete: ${processed} processed, ${failed} failed`,
    );

    return { processed, failed };
  }

  /**
   * Remove uma mensagem da DLQ (descarta permanentemente)
   */
  async discardMessage(orderId: string): Promise<boolean> {
    const channel = this.rabbitmqService.getChannel();
    if (!channel) {
      throw new Error('RabbitMQ channel not available');
    }

    const stats = await this.getStats();
    let found = false;

    for (let i = 0; i < stats.messageCount; i++) {
      const msg = await channel.get(this.DLQ_NAME, { noAck: false });

      if (!msg) break;

      try {
        const content = JSON.parse(
          msg.content.toString(),
        ) as PaymentOrderMessage;

        if (content.orderId === orderId) {
          // Encontrou! Remove permanentemente (ack sem reprocessar)
          channel.ack(msg);
          found = true;
          this.logger.warn(`🗑️ Message ${orderId} discarded from DLQ`);
          break;
        } else {
          // Não é a mensagem, devolve
          channel.nack(msg, false, true);
        }
      } catch {
        channel.nack(msg, false, true);
      }
    }

    return found;
  }

  /**
   * Limpa toda a DLQ (CUIDADO!)
   */
  async purgeAll(): Promise<number> {
    const channel = this.rabbitmqService.getChannel();
    if (!channel) {
      throw new Error('RabbitMQ channel not available');
    }

    const result = await channel.purgeQueue(this.DLQ_NAME);

    this.logger.warn(`🗑️ Purged ${result.messageCount} messages from DLQ`);

    return result.messageCount;
  }
}
