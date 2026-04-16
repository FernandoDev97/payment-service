import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service';
import { ConfigModule } from '@nestjs/config';
import { PaymentQueueService } from './payment-queue/payment-queue.service';
import { PaymentConsumerService } from './payment-consumer/payment-consumer.service';

@Module({
  imports: [ConfigModule],
  providers: [RabbitmqService, PaymentQueueService, PaymentConsumerService],
})
export class EventsModule {}
