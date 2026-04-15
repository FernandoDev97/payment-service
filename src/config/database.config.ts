import { TypeOrmModuleOptions } from '@nestjs/typeorm';

export const databaseConfig: TypeOrmModuleOptions = {
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: Number(process.env.DB_PORT) || 5435,
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'payments_db',
  entities: [__dirname + '/../**/*.entity{.ts,js}'],
  synchronize: process.env.NODE_ENV !== 'production', // Apenas em dev
  logging: process.env.NODE_ENV === 'development',
};
