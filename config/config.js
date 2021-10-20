import {env} from 'node:process';
import {config as _config} from 'dotenv';

_config();

export const db = {
  url: env.DB_URL,
  name: env.DB_NAME,
};
