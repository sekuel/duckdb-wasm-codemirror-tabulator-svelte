<script>
	import { onMount } from 'svelte';
	import { CodeMirror, sql } from '$lib/js/codemirror';
	import { initDB, duckdb } from '$lib/js/duckdb.js';
	import { TabulatorFull as Tabulator } from 'tabulator-tables';

	let conn;
	let conn_prom;
	onMount(async () => {
		conn_prom = load_db();
	});

	async function load_db() {
		try {
			const db = await initDB();
			conn = await db.connect(':memory:');
			return conn;
		} catch (error) {
			console.error('Failed to initialize database:', error);
			throw error; // Rethrow the error to propagate it further
		}
	}

	async function execute(query) {
		const urlPattern = /(https?:\/\/[^\s\/$.?#].[^\s]*\.json)/i;
		const match = query.match(urlPattern);
		if (match) {
			const url = match[1];
			console.log('JSON URL:', url);
			try {
				let response = await fetch(url);
				let json = await response.json();
				const db = await initDB();
				await db.registerFileText(url, JSON.stringify(json));
				await conn.insertJSONFromPath(url, { schema: 'main', name: url });
			} catch (error) {
				results = new Promise((resolve, reject) => reject(error));
			}
		}
		try {
			const conn = await conn_prom;
			const res = await conn.query(query);
			results = {
				rows: res.toArray().map((r) => Object.fromEntries(r)),
				columns: res.schema.fields.map((r) => ({ title: r.name, field: r.name }))
			};
		} catch (error) {
			results = new Promise((resolve, reject) => reject(error));
		}
	}

	function tableAction(node, { data, columns }) {
		let table = new Tabulator(node, {
			height: '30rem',
			data,
			columns
		});

		return {
			update: ({ data, columns }) => {
				table.setColumns(columns);
				table.setData(data);
			}
		};
	}

	async function createTableFromFiles(file) {
		const db = await initDB();
		const fileExt = file.name.substring(file.name.lastIndexOf('.'));
		try {
			if (fileExt == '.parquet') {
				await db.registerFileHandle(
					file.name,
					file,
					duckdb.DuckDBDataProtocol.BROWSER_FILEREADER,
					true
				);
			} else if (fileExt == '.json') {
				let json = await file.text();
				await db.registerFileText(file.name, JSON.stringify(JSON.parse(json)));
				await conn.insertJSONFromPath(file.name, { schema: 'main', name: file.name });
			} else if (fileExt == '.csv') {
				let csv = await file.text();
				await db.registerFileText(file.name, csv);
			}
			await conn.query(`CREATE OR REPLACE TABLE '${file.name}' AS (SELECT * FROM '${file.name}')`);
			tables = getTables();

		} catch (error) {
			results = new Promise((resolve, reject) => reject(error));
		}
	}

	async function fetchData(query) {
		const db = await initDB();
		const conn = await db.connect();
		conn.send(`COPY (${query}) TO 'result-snappy.parquet' (FORMAT 'parquet');`);
		const parquet_buffer = await db.copyFileToBuffer('result-snappy.parquet');
		let fileUrl = URL.createObjectURL(new Blob([parquet_buffer]));
		let fileName = 'result-snappy.parquet';

		await conn.close();

		const link = document.createElement('a');
		link.href = fileUrl;
		link.download = fileName;
		link.click();
	}

	async function getTables() {
		const db = await initDB();
		const conn = await db.connect();
		const res = await conn.query('SHOW TABLES');
		tables = {
			rows: res.toArray().map((r) => Object.fromEntries(r)),
			columns: res.schema.fields.map((r) => ({ title: r.name, field: r.name }))
		};
	}

	let files;
	$: tables = new Promise(() => ({}));

	$: if (files) {
		for (const file of files) {
			createTableFromFiles(file);
		}
	}
	$: results = new Promise(() => ({}));
	$: value = 'SELECT * FROM duckdb_functions()';
	$: placeholder = '';
</script>

<title>DuckDB-Wasm Playground with CodeMirror 6 & Tabulator</title>
<div class="main">
	<h1>DuckDB-Wasm Playground with CodeMirror 6 & Tabulator</h1>
	<input
		bind:files
		id="many"
		multiple
		type="file"
		accept=".json, .parquet, .csv"
		title="Query your local parquet, csv, json. Your data will not be sent out of the device you are using."
	/>
	{#await tables then table}
		<code>
			<p>Available tables:</p>
			<ul>
				{#each Array.from(table.rows) as t}
					<li>'{t.name}'</li>
				{/each}
			</ul>
		</code>
	{/await}

	<CodeMirror
		bind:value
		bind:placeholder
		lang={sql()}
		styles={{ '&': { maxWidth: '100%', height: '20rem' } }}
	/>

	<button
		on:click={() => {
			execute(value);
		}}
		title="Execute Query"
	>
		Execute
	</button>
	<button on:click={fetchData(value)} title="Export Query as Parquet File">Export as Parquet</button
	>

	<!-- <div class="file-upload">
		<label for="many">Upload local files (parquet, json, csv):</label>
	</div> -->
	{#await results then r}
		<div use:tableAction={{ data: r.rows, columns: r.columns }} />
	{:catch error}
		<p style="color: red">{error.message}</p>
	{/await}
</div>

<style>
	.main {
		font-family: 'Atkinson Hyperlegible', sans-serif;
		font-weight: 400;
		margin: 0 auto;
		padding: 1rem;
		max-width: 768px;
	}

	button,
	input[type='file']::file-selector-button {
		border: 1;
		cursor: pointer;
		color: #3c4257;
		background-color: #ffffff;
		border-radius: 4px;
		margin-top: 1rem;
		margin-bottom: 1rem;
		padding: 4px 8px;
		display: inline-block;
		min-height: 28px;
	}
</style>
