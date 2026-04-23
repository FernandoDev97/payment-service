import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service';
import { ConfigModule } from '@nestjs/config';
import { PaymentQueueService } from './payment-queue/payment-queue.service';
import { PaymentConsumerService } from './payment-consumer/payment-consumer.service';
import { DlqService } from './dlq/dlq.service';
import { DlqController } from './dlq/dlq.controller';

@Module({
  imports: [ConfigModule],
  controllers: [DlqController],
  providers: [
    RabbitmqService,
    PaymentQueueService,
    PaymentConsumerService,
    DlqService,
  ],
})
export class EventsModule {}
