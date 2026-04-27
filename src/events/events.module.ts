import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service';
import { ConfigModule } from '@nestjs/config';
import { PaymentQueueService } from './payment-queue/payment-queue.service';
import { PaymentConsumerService } from './payment-consumer/payment-consumer.service';
import { DlqService } from './dlq/dlq.service';
import { DlqController } from './dlq/dlq.controller';
import { MetricsController } from './metrics/metrics.controller';
import { MetricsService } from './metrics/metrics.service';

@Module({
  imports: [ConfigModule],
  controllers: [DlqController, MetricsController],
  providers: [
    RabbitmqService,
    PaymentQueueService,
    PaymentConsumerService,
    DlqService,
    MetricsService,
  ],
})
export class EventsModule {}
