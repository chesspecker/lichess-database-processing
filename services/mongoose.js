import mongoose from 'mongoose';
import {db} from '../config/config.js';

mongoose.connection.on('connected', () => {
	console.log('MongoDB is connected');
});

mongoose.connection.on('error', error => {
	console.log(`Could not connect to MongoDB because of ${error}`);
	throw error;
});

export function connect() {
	mongoose.connect(db.url, {
		keepAlive: true,
		useNewUrlParser: true,
		dbName: db.name,
	});

	return mongoose.connection;
}
