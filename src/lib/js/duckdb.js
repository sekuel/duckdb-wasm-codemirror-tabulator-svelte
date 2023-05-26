import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import duckdb_worker from '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?worker';

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

export { initDB };