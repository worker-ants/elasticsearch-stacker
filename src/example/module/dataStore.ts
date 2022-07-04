import { createPool, Pool } from 'mysql2/promise';
import { MysqlConfig } from '../config/mysql';

export class DataStore {
  private readonly pool: Pool;

  constructor(config: MysqlConfig) {
    this.pool = createPool(config);

    // force charset (character-set-client-handshake=OFF || skip-character-set-client-handshake)
    this.getPool().on('connection', (connection) => {
      connection.query(`SET NAMES ${config.charset}`);
    });
  }

  public getPool(): Pool {
    return this.pool;
  }
}
