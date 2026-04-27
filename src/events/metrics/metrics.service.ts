import { Injectable, Logger } from '@nestjs/common';

export interface ConsumerMetrics {
  totalProcessed: number;
  totalSuccess: number;
  totalFailed: number;
  totalRetries: number;
  lastProcessedAt: Date | null;
  startedAt: Date;
  averageProcessingTime: number;
}

@Injectable()
export class MetricsService {
  private metrics: ConsumerMetrics = {
    totalProcessed: 0,
    totalSuccess: 0,
    totalFailed: 0,
    totalRetries: 0,
    lastProcessedAt: null,
    startedAt: new Date(),
    averageProcessingTime: 0,
  };

  private totalProcessingTime = 0;

  private readonly logger = new Logger(MetricsService.name);

  markConsumerStarted(): void {
    this.metrics.startedAt = new Date();
  }

  updateMetrics(success: boolean, startTime: number): void {
    const processingTime = Date.now() - startTime;
    this.metrics.totalProcessed++;
    this.metrics.lastProcessedAt = new Date();

    if (success) {
      this.metrics.totalSuccess++;
    } else {
      this.metrics.totalFailed++;
    }

    this.totalProcessingTime += processingTime;
    this.metrics.averageProcessingTime = Math.round(
      this.totalProcessingTime / this.metrics.totalProcessed,
    );

    if (this.metrics.totalProcessed % 10 === 0) {
      this.logMetricsSummary();
    }
  }

  incrementRetryCount(): void {
    this.metrics.totalRetries++;
  }

  getMetrics(): ConsumerMetrics {
    return { ...this.metrics };
  }

  resetMetrics(): void {
    this.metrics = {
      totalProcessed: 0,
      totalSuccess: 0,
      totalFailed: 0,
      totalRetries: 0,
      lastProcessedAt: null,
      startedAt: new Date(),
      averageProcessingTime: 0,
    };
    this.totalProcessingTime = 0;
    this.logger.log('📊 Métricas do consumidor resetadas');
  }

  private logMetricsSummary(): void {
    const successRate =
      this.metrics.totalProcessed > 0
        ? (this.metrics.totalSuccess / this.metrics.totalProcessed) * 100
        : 0;

    this.logger.log(
      `📊 Métricas do consumidor: ` +
        `Total Processados=${this.metrics.totalProcessed}, ` +
        `Sucessos=${this.metrics.totalSuccess}, ` +
        `Falhas=${this.metrics.totalFailed}, ` +
        `Tempo Médio=${this.metrics.averageProcessingTime}ms, ` +
        `Taxa de Sucesso=${successRate.toFixed(2)}%`,
    );
  }
}
