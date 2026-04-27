import { Controller, Get, Logger, Post } from '@nestjs/common';
import { ConsumerMetrics, MetricsService } from './metrics.service';

@Controller('metrics')
export class MetricsController {
  private readonly logger = new Logger(MetricsController.name);

  constructor(private readonly metricsService: MetricsService) {}

  @Get()
  getMetrics(): ConsumerMetrics & {
    successRate: string;
    uptime: string;
    status: string;
  } {
    const metrics = this.metricsService.getMetrics();

    // Calcula taxa de sucesso
    const successRate =
      metrics.totalProcessed > 0
        ? ((metrics.totalSuccess / metrics.totalProcessed) * 100).toFixed(2) +
          '%'
        : '0%';
    // Calcula uptime
    const uptime = this.calculateUptime(metrics.startedAt);

    return {
      ...metrics,
      successRate,
      uptime,
      status: 'active',
    };
  }

  @Get('health')
  getHealth(): {
    status: 'healthy' | 'unhealthy' | 'degraded';
    checks: Record<string, boolean>;
    message: string;
    timestamp: string;
  } {
    const metrics = this.metricsService.getMetrics();

    // Verifica se está processando (ativo nos últimos 5 min)
    const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
    const isProcessing =
      !metrics.lastProcessedAt ||
      metrics.lastProcessedAt.getTime() > fiveMinutesAgo;

    // Verifica taxa de sucesso (>= 90%)

    const successRate =
      metrics.totalProcessed > 0
        ? (metrics.totalSuccess / metrics.totalProcessed) * 100
        : 100;
    const hasGoodSuccessRate = successRate >= 90;

    // Verifica se não há muitas falhas consecutivas
    const hasLowFailures = metrics.totalFailed < 100;

    // Determina status
    let status: 'healthy' | 'unhealthy' | 'degraded' = 'healthy';
    let message = 'Consumer is operating normally';

    if (!isProcessing && metrics.totalProcessed > 0) {
      status = 'unhealthy';
      message = 'Consumer has not processed messages in the last 5 minutes';
    } else if (!hasGoodSuccessRate) {
      status = 'degraded';
      message = `Success rate is below 90% (${successRate.toFixed(2)}%)`;
    } else if (!hasLowFailures) {
      status = 'degraded';
      message = 'High number of failed messages';
    }

    return {
      status,
      checks: { isProcessing, hasGoodSuccessRate, hasLowFailures },
      message,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('summary')
  getSummary(): {
    processed: number;
    success: number;
    failed: number;
    rate: string;
    avgTime: string;
  } {
    const metrics = this.metricsService.getMetrics();

    return {
      processed: metrics.totalProcessed,
      success: metrics.totalSuccess,
      failed: metrics.totalFailed,
      rate:
        metrics.totalProcessed > 0
          ? ((metrics.totalSuccess / metrics.totalProcessed) * 100).toFixed(1) +
            '%'
          : '0%',
      avgTime: `${metrics.averageProcessingTime}ms`,
    };
  }

  @Post('reset')
  resetMetrics(): { success: boolean; message: string } {
    this.metricsService.resetMetrics();
    this.logger.warn('⚠️ Metrics were reset by API call');

    return {
      success: true,
      message: 'Metrics reset successfully',
    };
  }

  private calculateUptime(startedAt: Date): string {
    const diff = Date.now() - startedAt.getTime();

    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) {
      return `${days}d ${hours % 24}h ${minutes % 60}m`;
    } else if (hours > 0) {
      return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
      return `${minutes % 60}m ${seconds % 60}s`;
    } else {
      return `${seconds % 60}s`;
    }
  }
}
