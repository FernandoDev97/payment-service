export interface PaymentOrderMessage {
  orderId: string;
  userId: string;
  amount: number;
  items: Array<{
    productId: string;
    quantity: number;
    price: number;
  }>;
  paymentMethod: string;
  description?: string;
  createdAt?: Date;
  metadata?: {
    service: string;
    timestamp: string;
  };
}
