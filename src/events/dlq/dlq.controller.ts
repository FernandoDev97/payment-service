import {
  Controller,
  Get,
  Post,
  Delete,
  Param,
  Query,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { DlqService, DLQMessage, DLQStats } from './dlq.service';

@Controller('dlq')
export class DlqController {
  private readonly logger = new Logger(DlqController.name);

  constructor(private readonly dlqService: DlqService) {}

  /**
   * GET /dlq/stats
   * Retorna estatísticas da DLQ
   */
  @Get('stats')
  async getStats(): Promise<DLQStats> {
    try {
      return await this.dlqService.getStats();
    } catch (error) {
      this.logger.error('Failed to get DLQ stats:', error);
      throw new HttpException(
        'Failed to get DLQ stats',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * GET /dlq/messages?limit=10
   * Lista mensagens na DLQ (sem remover)
   */
  @Get('messages')
  async getMessages(
    @Query('limit') limit?: string,
  ): Promise<{ count: number; messages: DLQMessage[] }> {
    try {
      const parsedLimit = limit ? parseInt(limit, 10) : 10;
      const messages = await this.dlqService.peekMessages(parsedLimit);

      return {
        count: messages.length,
        messages,
      };
    } catch (error) {
      this.logger.error('Failed to get DLQ messages:', error);
      throw new HttpException(
        'Failed to get DLQ messages',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * POST /dlq/reprocess/:orderId
   * Reprocessa uma mensagem específica
   */
  @Post('reprocess/:orderId')
  async reprocessMessage(
    @Param('orderId') orderId: string,
  ): Promise<{ success: boolean; message: string }> {
    try {
      const found = await this.dlqService.reprocessMessage(orderId);

      if (!found) {
        throw new HttpException(
          `Message with orderId ${orderId} not found in DLQ`,
          HttpStatus.NOT_FOUND,
        );
      }

      return {
        success: true,
        message: `Message ${orderId} sent back to main queue for reprocessing`,
      };
    } catch (error) {
      if (error instanceof HttpException) throw error;

      this.logger.error(`Failed to reprocess message ${orderId}:`, error);
      throw new HttpException(
        'Failed to reprocess message',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * POST /dlq/reprocess-all
   * Reprocessa todas as mensagens da DLQ
   */
  @Post('reprocess-all')
  async reprocessAll(): Promise<{
    success: boolean;
    processed: number;
    failed: number;
  }> {
    try {
      const result = await this.dlqService.reprocessAll();

      return {
        success: true,
        ...result,
      };
    } catch (error) {
      this.logger.error('Failed to reprocess all messages:', error);
      throw new HttpException(
        'Failed to reprocess all messages',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * DELETE /dlq/message/:orderId
   * Remove uma mensagem da DLQ (descarta permanentemente)
   */
  @Delete('message/:orderId')
  async discardMessage(
    @Param('orderId') orderId: string,
  ): Promise<{ success: boolean; message: string }> {
    try {
      const found = await this.dlqService.discardMessage(orderId);

      if (!found) {
        throw new HttpException(
          `Message with orderId ${orderId} not found in DLQ`,
          HttpStatus.NOT_FOUND,
        );
      }

      return {
        success: true,
        message: `Message ${orderId} permanently discarded from DLQ`,
      };
    } catch (error) {
      if (error instanceof HttpException) throw error;

      this.logger.error(`Failed to discard message ${orderId}:`, error);
      throw new HttpException(
        'Failed to discard message',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * DELETE /dlq/purge
   * Remove TODAS as mensagens da DLQ (CUIDADO!)
   */
  @Delete('purge')
  async purgeAll(): Promise<{ success: boolean; purgedCount: number }> {
    try {
      const count = await this.dlqService.purgeAll();

      return {
        success: true,
        purgedCount: count,
      };
    } catch (error) {
      this.logger.error('Failed to purge DLQ:', error);
      throw new HttpException(
        'Failed to purge DLQ',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
