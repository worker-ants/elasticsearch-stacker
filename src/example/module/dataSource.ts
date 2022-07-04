import { createPool, Pool } from 'mysql2/promise';
import { MysqlConfig } from '../config/mysql';

export class DataSource {
  private readonly pool: Pool;

  constructor(config: MysqlConfig) {
    this.pool = createPool({
      ...config,
      queryFormat: function (query: string, values: any) {
        if (!values) return query;
        return query.replace(
          /:(\w+)/g,
          function (txt, key) {
            if (values.hasOwnProperty(key)) {
              return this.escape(values[key]);
            }
            return txt;
          }.bind(this),
        );
      },
    });

    this.getPool().on('connection', (connection) => {
      connection.query(`SET NAMES ${config.charset}`);
    });
  }

  public getPool(): Pool {
    return this.pool;
  }
}
