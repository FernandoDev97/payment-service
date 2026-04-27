import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PaymentQueueService } from '../payment-queue/payment-queue.service';
import { PaymentOrderMessage } from '../payment-queue.interface';
import { RabbitmqService } from '../rabbitmq/rabbitmq.service';
import { ConsumerMetrics, MetricsService } from '../metrics/metrics.service';

@Injectable()
export class PaymentConsumerService implements OnModuleInit {
  private readonly logger = new Logger(PaymentConsumerService.name);

  constructor(
    private readonly paymentQueueService: PaymentQueueService,
    private readonly rabbitMQService: RabbitmqService,
    private readonly metricsService: MetricsService,
  ) {}

  async onModuleInit() {
    this.logger.log('Iniciando serviço de consumo de ordens de pagamento');
    this.metricsService.markConsumerStarted();
    await this.startConsuming();
  }

  async startConsuming() {
    try {
      this.logger.log('Iniciando consumo de ordens de pagamento');
      const isConnected = await this.rabbitMQService.waitForConnection();

      if (!isConnected) {
        this.logger.error(
          '❌ Falha ao iniciar serviço de consumo de ordens de pagamento',
        );
        return;
      }

      // Registra callback para processar cada mensagem
      // O bind(this) garante que o 'this' dentro do callback seja esta classe
      await this.paymentQueueService.consumePaymentOrders(
        this.processPaymentOrder.bind(this),
      );

      this.logger.log(
        '✅ Serviço de consumo de ordens de pagamento iniciado com sucesso',
      );
    } catch (error) {
      this.logger.error(
        '❌ Falha ao iniciar serviço de consumo de ordens de pagamento:',
        error,
      );
    }
  }

  private processPaymentOrder(message: PaymentOrderMessage): void {
    const startTime = Date.now();
    try {
      // Log inicial com informações da mensagem
      this.logger.log(
        `📝 Processando ordem de pagamento: ` +
          `orderId=${message.orderId}, ` +
          `userId=${message.userId}, ` +
          `amount=${message.amount}`,
      );

      // Validar mensagem antes de processar
      if (!this.validateMessage(message)) {
        this.logger.error('❌ Mensagem inválida');
        // Rejeitamos a mensagem para não ficar reprocessando
        throw new Error('Messagem invalida para processamento de pagamento');
      }

      // TODO: Processar pagamento usando PaymentsService
      // Isso será implementado na próxima aula
      this.logger.log('✅ Pagamento recebido e validado');
      this.metricsService.updateMetrics(true, startTime);
    } catch (error) {
      this.metricsService.updateMetrics(false, startTime);
      // Log de erro com contexto completo
      this.logger.error(
        `❌ Falha ao processar pagamento para o pedido ${message.orderId}:`,
        error,
      );

      // IMPORTANTE: Relançamos o erro para o RabbitMQ fazer NACK
      throw error;
    }
  }

  private validateMessage(message: PaymentOrderMessage): boolean {
    // Verificações básicas
    if (!message.orderId) {
      this.logger.error('❌ orderId ausente na mensagem de pagamento');
      return false;
    }

    if (!message.userId) {
      this.logger.error('❌ userId ausente na mensagem de pagamento');
      return false;
    }

    if (!message.amount || message.amount <= 0) {
      this.logger.error('❌ amount inválido na mensagem de pagamento');
      return false;
    }

    if (!message.paymentMethod) {
      this.logger.error('❌ paymentMethod ausente na mensagem de pagamento');
      return false;
    }

    // Validação dos itens
    if (!message.items || message.items.length === 0) {
      this.logger.error('❌ items ausentes na mensagem de pagamento');
      return false;
    }

    // Todas validações passaram
    return true;
  }

  incrementRetryCount(): void {
    this.metricsService.incrementRetryCount();
  }

  getMetrics(): ConsumerMetrics {
    return this.metricsService.getMetrics();
  }

  resetMetrics(): void {
    this.metricsService.resetMetrics();
  }
}
