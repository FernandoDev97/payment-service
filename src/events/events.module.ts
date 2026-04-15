import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service';
import { ConfigModule } from '@nestjs/config';
import { PaymentQueueService } from './payment-queue/payment-queue.service';

@Module({
  imports: [ConfigModule],
  providers: [RabbitmqService, PaymentQueueService],
})
export class EventsModule {}
