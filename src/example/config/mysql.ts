import 'dotenv/config';

declare type TimeZone = 'Asia/Seoul';
export interface MysqlConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  timezone: TimeZone;
}
export const mysqlConfig: MysqlConfig = {
  host: process.env?.MYSQL_HOST,
  port: parseInt(process.env?.MYSQL_PORT || '3306', 10),
  database: process.env?.MYSQL_DATABASE,
  user: process.env?.MYSQL_USER,
  password: process.env?.MYSQL_PASSWORD,
  timezone: process.env?.MYSQL_TIMEZONE as TimeZone,
};
