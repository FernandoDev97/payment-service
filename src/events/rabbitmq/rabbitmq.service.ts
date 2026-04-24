import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import * as amqp from 'amqplib';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class RabbitmqService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitmqService.name);
  private connection!: amqp.ChannelModel;
  private channel!: amqp.Channel;

  private static readonly MAIN_QUEUE_TTL_MS = 86400000;
  private static readonly DLQ_TTL_MS = 604800000;
  private static readonly MAIN_QUEUE_MAX_LENGTH = 10000;

  constructor(private configService: ConfigService) {}

  async onModuleInit() {
    await this.connect();
  }

  async onModuleDestroy() {
    await this.disconnect();
  }

  async waitForConnection(maxAttempts = 10, delayMs = 500): Promise<boolean> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (this.channel) {
        return true;
      }
      this.logger.log(
        `⏳ Aguardando conexão com o RabbitMQ... (tentativa ${attempt}/${maxAttempts})`,
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    return false;
  }

  private async connect() {
    try {
      const rabbitmqUrl = this.configService.get<string>(
        'RABBITMQ_URL',
        'amqp://admin:admin@localhost:5672',
      );

      this.connection = await amqp.connect(rabbitmqUrl);
      this.channel = await this.connection.createChannel();
      this.logger.log('✅ Conectado ao RabbitMQ com sucesso');

      // Event listener para monitorar a conexão
      this.connection.on('error', (err) => {
        this.logger.error('❌ Erro na conexão com o RabbitMQ:', err);
      });

      this.connection.on('close', () => {
        this.logger.warn('⚠️ Conexão com o RabbitMQ encerrada');
      });

      this.connection.on('blocked', (reason) => {
        this.logger.warn('⚠️ Conexão com o RabbitMQ bloqueada:', reason);
      });

      this.connection.on('unblocked', () => {
        this.logger.log('✅ Conexão com o RabbitMQ desbloqueada');
      });
    } catch (error) {
      this.logger.warn(
        '⚠️ Falha ao conectar no RabbitMQ, continuando sem fila de mensagens:',
        (error instanceof Error ? error.message : String(error)) || error,
      );
    }
  }

  private async disconnect() {
    try {
      if (this.channel) {
        await this.channel.close();
        this.logger.log('✅ Canal do RabbitMQ encerrado');
      }

      if (this.connection) {
        await this.connection.close();
        this.logger.log('✅ Desconectado do RabbitMQ');
      }
    } catch (error) {
      this.logger.error('❌ Erro ao desconectar do RabbitMQ:', error);
    }
  }

  getChannel(): amqp.Channel {
    return this.channel;
  }

  getConnection(): amqp.ChannelModel {
    return this.connection;
  }

  async publishMessage(
    exchange: string,
    routingKey: string,
    message: any,
  ): Promise<void> {
    try {
      if (!this.channel) {
        this.logger.warn(
          '⚠️ Canal do RabbitMQ indisponível, ignorando publicação da mensagem',
        );

        return;
      }

      await this.channel.assertExchange(exchange, 'topic', { durable: true });
      const messageBuffer = Buffer.from(JSON.stringify(message));

      const published = this.channel.publish(
        exchange,
        routingKey,
        messageBuffer,
        {
          persistent: true,
          timestamp: Date.now(),
          contentType: 'application/json',
        },
      );

      this.logger.log(`✅ Mensagem publicada em ${exchange}:${routingKey}`);
      this.logger.debug(`Conteúdo da mensagem: ${JSON.stringify(message)}`);
      if (!published) {
        throw new Error('Failed to publish message to RabbitMQ');
      }
    } catch (error) {
      this.logger.error('❌ Erro ao publicar mensagem no RabbitMQ:', error);
    }
  }

  async subscribeToQueue(
    queueName: string,
    exchange: string,
    routingKey: string,
    callback: (message: unknown) => Promise<void>,
    options: {
      maxRetries?: number; // Máximo de tentativas (padrão: 3)
      retryDelayMs?: number; // Delay entre retries (padrão: 30000ms)
    } = {},
  ): Promise<void> {
    const maxRetries = options.maxRetries ?? 3;
    const retryDelayMs = options.retryDelayMs ?? 30000; // 30 segundos

    try {
      this.ensureChannel();

      const { retryExchange, dlxExchange } =
        await this.setupExchanges(exchange);

      const dlqName = await this.setupDlqQueue(
        queueName,
        dlxExchange,
        routingKey,
      );

      const { retryQueueName, routingKeyRetry } = await this.setupRetryQueue(
        queueName,
        exchange,
        routingKey,
        retryExchange,
        retryDelayMs,
      );

      const mainQueueName = await this.setupMainQueue(
        queueName,
        exchange,
        routingKey,
        retryExchange,
        routingKeyRetry,
      );

      await this.startQueueConsumer({
        queueName,
        mainQueueName,
        callback,
        maxRetries,
        retryDelayMs,
        dlxExchange,
        routingKey,
      });

      this.logger.log(`✅ Inscrito na fila: ${queueName}`);
      this.logger.log(
        `🔄 Fila de retry: ${retryQueueName} (atraso de ${retryDelayMs}ms)`,
      );
      this.logger.log(`💀 Fila de mensagens mortas (DLQ): ${dlqName}`);
    } catch (error) {
      this.logger.error(`❌ Erro ao assinar a fila ${queueName}:`, error);
    }
  }

  private ensureChannel(): void {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not available');
    }
  }

  private async setupExchanges(
    exchange: string,
  ): Promise<{ retryExchange: string; dlxExchange: string }> {
    await this.channel.assertExchange(exchange, 'topic', { durable: true });

    const retryExchange = `${exchange}.retry.dlx`;
    await this.channel.assertExchange(retryExchange, 'topic', {
      durable: true,
    });

    const dlxExchange = `${exchange}.dlx`;
    await this.channel.assertExchange(dlxExchange, 'topic', {
      durable: true,
    });

    return { retryExchange, dlxExchange };
  }

  private async setupDlqQueue(
    queueName: string,
    dlxExchange: string,
    routingKey: string,
  ): Promise<string> {
    const dlqName = `${queueName}.dlq`;

    await this.channel.assertQueue(dlqName, {
      durable: true,
      arguments: {
        'x-message-ttl': RabbitmqService.DLQ_TTL_MS,
      },
    });

    const routingKeyDlq = `${routingKey}.dlq`;
    await this.channel.bindQueue(dlqName, dlxExchange, routingKeyDlq);

    return dlqName;
  }

  private async setupRetryQueue(
    queueName: string,
    exchange: string,
    routingKey: string,
    retryExchange: string,
    retryDelayMs: number,
  ): Promise<{ retryQueueName: string; routingKeyRetry: string }> {
    const routingKeyRetry = `${routingKey}.retry`;
    const retryQueueName = `${queueName}.retry`;

    await this.channel.assertQueue(retryQueueName, {
      durable: true,
      arguments: {
        'x-message-ttl': retryDelayMs,
        'x-dead-letter-exchange': exchange,
        'x-dead-letter-routing-key': routingKey,
      },
    });

    await this.channel.bindQueue(
      retryQueueName,
      retryExchange,
      routingKeyRetry,
    );

    return { retryQueueName, routingKeyRetry };
  }

  private async setupMainQueue(
    queueName: string,
    exchange: string,
    routingKey: string,
    retryExchange: string,
    routingKeyRetry: string,
  ): Promise<string> {
    const queue = await this.channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        'x-message-ttl': RabbitmqService.MAIN_QUEUE_TTL_MS,
        'x-max-length': RabbitmqService.MAIN_QUEUE_MAX_LENGTH,
        'x-dead-letter-exchange': retryExchange,
        'x-dead-letter-routing-key': routingKeyRetry,
      },
    });

    await this.channel.bindQueue(queue.queue, exchange, routingKey);

    return queue.queue;
  }

  private async startQueueConsumer(params: {
    queueName: string;
    mainQueueName: string;
    callback: (message: unknown) => Promise<void>;
    maxRetries: number;
    retryDelayMs: number;
    dlxExchange: string;
    routingKey: string;
  }): Promise<void> {
    const {
      queueName,
      mainQueueName,
      callback,
      maxRetries,
      retryDelayMs,
      dlxExchange,
      routingKey,
    } = params;

    await this.channel.prefetch(1);

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    await this.channel.consume(mainQueueName, async (msg) => {
      if (!msg) {
        return;
      }

      try {
        const message: unknown = JSON.parse(msg.content.toString());
        this.logger.log(`📨 Mensagem recebida da fila: ${queueName}`);
        this.logger.debug(`Conteúdo da mensagem: ${JSON.stringify(message)}`);

        const retryCount = this.getRetryCount(msg);

        this.logger.log(
          `📨 Mensagem recebida (tentativa ${retryCount + 1}/${maxRetries + 1})`,
        );

        await callback(message);

        this.channel.ack(msg);

        this.logger.log(
          `✅ Mensagem processada com sucesso da fila: ${queueName}`,
        );
      } catch (error) {
        this.handleProcessingError({
          msg,
          maxRetries,
          retryDelayMs,
          dlxExchange,
          routingKey,
        });
      }
    });
  }

  private handleProcessingError(params: {
    msg: amqp.ConsumeMessage;
    maxRetries: number;
    retryDelayMs: number;
    dlxExchange: string;
    routingKey: string;
  }): void {
    const { msg, maxRetries, retryDelayMs, dlxExchange, routingKey } = params;
    const retryCount = this.getRetryCount(msg);

    if (retryCount < maxRetries) {
      this.logger.warn(
        `⚠️ Falha no processamento (tentativa ${retryCount + 1}/${maxRetries + 1}). ` +
          `Tentando novamente em ${retryDelayMs / 1000}s...`,
      );
      this.channel.nack(msg, false, false);
      return;
    }

    this.logger.error(
      `💀 Máximo de tentativas (${maxRetries}) excedido. Enviando para a DLQ.`,
    );

    this.channel.publish(dlxExchange, `${routingKey}.dlq`, msg.content, {
      persistent: true,
      headers: msg.properties.headers,
    });
    this.channel.ack(msg);
  }

  /**
   * Extrai o número de retries do header x-death
   * O RabbitMQ adiciona esse header automaticamente
   */
  private getRetryCount(msg: amqp.ConsumeMessage): number {
    const xDeath = msg.properties.headers?.['x-death'] as
      | Array<{
          count: number;
          queue: string;
        }>
      | undefined;

    if (!xDeath || xDeath.length === 0) {
      return 0;
    }

    // Soma todas as vezes que passou pela fila principal
    return xDeath
      .filter((death) => !death.queue.endsWith('.retry'))
      .reduce((sum, death) => sum + (death.count || 0), 0);
  }
}

/*
// Header x-death adicionado automaticamente pelo RabbitMQ
{
  "x-death": [
    {
      "count": 3,           // ← Número de vezes que foi rejeitada
      "reason": "rejected",
      "queue": "payment_queue",
      "time": 1737241200,
      "exchange": "payments.retry.dlx",
      "routing-keys": ["payment.order.retry"]
    }
  ]
}
*/
