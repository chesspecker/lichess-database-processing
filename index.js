import * as csv from 'fast-csv';
import {createReadStream} from 'fs';
import {Puzzle} from './models/puzzle-model.js';
import {connect} from './services/mongoose.js';

connect();

let count = 0;

/**
 * Because there is about 2mio puzzles in the CSV file it's complicated to process all the game at once.
 * It would take about 60 hours I guess.
 * If you have imported the first 500k puzzles you can set skip to 500_000 for example.
 */
let skip = 0;

/**
 * The first setTimeout leave time for mongoose to connect to the DB
 * The second setTimeout is draining operations
 */

setTimeout(async () => {
	const stream_ = createReadStream('./lichess_db_puzzle.csv')
		.pipe(
			csv.parse({
				headers: [
					'PuzzleId',
					'FEN',
					'Moves',
					'Rating',
					'RatingDeviation',
					'Popularity',
					'NbPlays',
					'Themes',
					'GameUrl',
				],
			}),
		)
		.on('error', error => console.error(error))
		.on('data', async row => {
			count = count + 1;
			if (count > skip) {
				stream_.pause();
				row['Themes'] = row['Themes'].split(' ');
				row['Rating'] = parseInt(row['Rating']);
				row['RatingDeviation'] = parseInt(row['RatingDeviation']);
				row['Popularity'] = parseInt(row['Popularity']);
				row['NbPlays'] = parseInt(row['NbPlays']);
				const pzl = new Puzzle(row);
				setTimeout(async () => {
					await pzl.save(error => {
						if (error) throw new Error(error);
						stream_.resume();
					});
				}, 50);
			}
		})
		.on('end', rowCount => console.log(`Parsed ${rowCount} rows`));
}, 1800);
