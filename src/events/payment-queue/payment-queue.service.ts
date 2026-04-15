import { Injectable, Logger } from '@nestjs/common';
import { RabbitmqService } from '../rabbitmq/rabbitmq.service';

@Injectable()
export class PaymentQueueService {
  private readonly logger = new Logger(PaymentQueueService.name);

  private readonly ROUTING_KEY = 'payment.order';
  private readonly EXCHANGE = 'payments';
  private readonly QUEUE_NAME = 'payment_queue';

  constructor(private readonly rabbitMQService: RabbitmqService) {}

  async consumePaymentOrders(
    callback: (message: any) => Promise<void>,
  ): Promise<void> {
    this.logger.log('📡 Configurando consumidor de ordens de pagamento...');

    await this.rabbitMQService.subscribeToQueue(
      this.QUEUE_NAME,
      this.EXCHANGE,
      this.ROUTING_KEY,
      callback,
    );

    this.logger.log('✅ Consumidor de ordens de pagamento está pronto');
  }
}
