import 'dotenv/config';

export const esConfig = {
  host: process.env?.ES_HOST,
  user: process.env?.ES_USER,
  password: process.env?.ES_PASSWORD,
};
