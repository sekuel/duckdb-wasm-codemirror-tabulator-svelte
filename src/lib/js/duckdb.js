import * as duckdb from '@braintrust/duckdb-wasm';
import duckdb_wasm from '/node_modules/@braintrust/duckdb-wasm/dist/duckdb-eh.wasm?url';
import duckdb_worker from '/node_modules/@braintrust/duckdb-wasm/dist/duckdb-browser-eh.worker.js?worker';

let db = null;

const initDB = async () => {
	if (db) {
		return db;
	}

	const logger = new duckdb.ConsoleLogger();
	const worker = new duckdb_worker();

	db = new duckdb.AsyncDuckDB(logger, worker);
	await db.instantiate(duckdb_wasm);
	return db;
};

export { initDB, duckdb };
