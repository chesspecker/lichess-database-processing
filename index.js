import {createReadStream} from 'node:fs';
import papa from 'papaparse';
import PQueue from 'p-queue';
import {Puzzle} from './models/puzzle-model.js';
import {connect} from './services/mongoose.js';

connect();

const options = {fastMode: true, delimiter: ','};
const sleep = async ms => new Promise(r => setTimeout(r, ms));

/**
 * Because there is about 2mio puzzles in the CSV file it's complicated to process all the game at once.
 * It would take about 60 hours I guess.
 * If you have imported the first 500k puzzles you can set skip to 500_000 for example.
 */
let count = 0;
const skip = 1555;

/**
 * Depending on the size of the CSV file and the amount of available memory you can set the concurrency to a lower value.
 */
const limit = 40;
const concurrency = 20;

const main = async () => {
	const dataStream = createReadStream('./lichess_db_puzzle.csv');
	const parseStream = papa.parse(papa.NODE_STREAM_INPUT, options);
	const queue = new PQueue({concurrency});

	queue.on('add', () => {
		if (queue.size > limit) parseStream.pause();
	});

	queue.on('next', () => {
		if (queue.size < limit) parseStream.resume();
	});

	dataStream.pipe(parseStream);
	parseStream.on('error', error => console.error(error));
	parseStream.on('data', async row => {
		count++;
		if (count < skip) return;
		const [
			PuzzleId,
			FEN,
			Moves,
			Rating,
			RatingDeviation,
			Popularity,
			NbPlays,
			Themes,
			GameUrl,
		] = row;

		const Rating_ = Number.parseInt(Rating, 10);
		const RatingDeviation_ = Number.parseInt(RatingDeviation, 10);
		const Popularity_ = Number.parseInt(Popularity, 10);
		const NbPlays_ = Number.parseInt(NbPlays, 10);
		const Themes_ = Themes.split(' ');

		await queue.add(
			() =>
				new Promise(async resolve => {
					const previous = await Puzzle.findOne({PuzzleId}).lean().exec();
					if (previous) {
						const old = {
							Rating: previous.Rating,
							RatingDeviation: previous.RatingDeviation,
							Popularity: previous.Popularity,
							NbPlays: previous.NbPlays,
							Themes: previous.Themes,
						};

						const update = {
							Rating: Rating_,
							RatingDeviation: RatingDeviation_,
							Popularity: Popularity_,
							NbPlays: NbPlays_,
							Themes: Themes_,
						};

						if (JSON.stringify(old) === JSON.stringify(update)) {
							console.log(`${count} ${PuzzleId} • Already good`);
							resolve();
							return;
						}

						await Puzzle.updateOne({PuzzleId}, {$set: update}).exec();
						console.log(
							`${count} ${PuzzleId} • Found and updated • ${queue.size}`,
						);
						resolve();
						return;
					}

					const puzzle = new Puzzle({
						PuzzleId,
						FEN,
						Moves,
						Rating: Rating_,
						RatingDeviation: RatingDeviation_,
						Popularity: Popularity_,
						NbPlays: NbPlays_,
						Themes: Themes_,
						GameUrl,
					});
					await puzzle.save();
					console.log(`${count} ${PuzzleId} • Created • ${queue.size}`);
					resolve();
				}),
		);
	});

	parseStream.on('end', rowCount => {
		console.log(`Parsed ${rowCount} rows`);
	});
};

sleep(1500).then(() => main());
