/* eslint-disable @typescript-eslint/no-misused-promises */
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
  private connection: amqp.ChannelModel;
  private channel: amqp.Channel;

  constructor(private configService: ConfigService) {}

  async onModuleInit() {
    await this.connect();
  }

  async onModuleDestroy() {
    await this.disconnect();
  }

  private async connect() {
    try {
      const rabbitmqUrl = this.configService.get<string>('RABBITMQ_URL');

      if (!rabbitmqUrl) {
        throw new Error(
          'RABBITMQ_URL não encontrada nas variáveis de ambiente',
        );
      }

      this.connection = await amqp.connect(rabbitmqUrl);
      this.channel = await this.connection.createChannel();
      this.logger.log('✅ Conectado ao RabbitMQ com sucesso');

      this.connection.on('error', (err) => {
        this.logger.error('❌ Erro de conexão com RabbitMQ:', err);
      });

      this.connection.on('close', () => {
        this.logger.warn('⚠️ Conexão com RabbitMQ fechada');
      });

      this.connection.on('blocked', (reason) => {
        this.logger.warn('⚠️ Conexão com RabbitMQ bloqueada:', reason);
      });

      this.connection.on('unblocked', () => {
        this.logger.log('✅ Conexão com RabbitMQ desbloqueada');
      });
    } catch (error) {
      this.logger.warn(
        '⚠️ Falha ao se conectar ao RabbitMQ, continuando sem fila de mensagens:',
        error.message || error,
      );
    }
  }

  private async disconnect() {
    try {
      if (this.channel) {
        await this.channel.close();
        this.logger.log('✅ Canal do RabbitMQ fechado');
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
          '⚠️ Canal do RabbitMQ indisponível, ignorando mensagem',
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

      if (!published) {
        throw new Error('Falha ao publicar mensagem no RabbitMQ');
      }

      this.logger.log(
        `✅ Mensagem publicada no exchange ${exchange} com routing key ${routingKey}`,
      );
      this.logger.debug(`Conteúdo da mensagem: ${JSON.stringify(message)}`);
    } catch (error) {
      this.logger.error('❌ Erro ao publicar mensagem:', error);
    }
  }

  async subscribeToQueue(
    queueName: string,
    exchange: string,
    routingKey: string,
    callback: (message: unknown) => Promise<void>,
  ): Promise<void> {
    try {
      if (!this.channel) {
        this.logger.warn(
          '⚠️ Canal do RabbitMQ indisponível, ignorando inscrição',
        );
        throw new Error('Canal do RabbitMQ indisponível');
      }

      await this.channel.assertExchange(exchange, 'topic', { durable: true });
      const queue = await this.channel.assertQueue(queueName, {
        durable: true,
        arguments: {
          'x-message-ttl': 60 * 60 * 24,
          'x-max-length': 10000,
        },
      });
      await this.channel.bindQueue(queue.queue, exchange, routingKey);
      await this.channel.prefetch(1);
      await this.channel.consume(queue.queue, async (msg) => {
        if (msg) {
          try {
            const message: unknown = JSON.parse(msg.content.toString());
            this.logger.log(
              `Mensagem recebida da fila ${queueName}: ${JSON.stringify(message)}`,
            );
            this.logger.debug(
              `Conteúdo da mensagem: ${JSON.stringify(message)}`,
            );
            await callback(message);
            this.channel.ack(msg);
            this.logger.log(
              `Mensagem processada com sucesso da fila: ${queueName}`,
            );
          } catch (error) {
            this.logger.error(
              `Erro ao processar mensagem da fila ${queueName}:`,
              error,
            );
            this.channel.nack(msg, false, false);
          }
        }
      });

      this.logger.log(
        `✅ Inscrito na fila ${queueName} com routing key ${routingKey}`,
      );
    } catch (error) {
      this.logger.error(`Erro ao se inscrever na fila ${queueName}:`, error);
    }
  }
}
