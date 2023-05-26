<script>
	import { onMount } from 'svelte';
	import { CodeMirror, sql } from '$lib/js/codemirror';
	import { initDB } from '$lib/js/duckdb.js';
	import { TabulatorFull as Tabulator } from 'tabulator-tables';

	async function load_db() {
		try {
			const db = await initDB();
			const conn = await db.connect();
			return conn;
		} catch (error) {
			console.error('Failed to initialize database:', error);
			throw error; // Rethrow the error to propagate it further
		}
	}

	async function execute(query) {
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

	// Initialize database connection on component mount
	let conn_prom;
	onMount(async () => {
		conn_prom = load_db();
	});

	$: results = new Promise(() => ({}));

	let value = 'SELECT * from duckdb_functions()';
</script>

<title>DuckDB-Wasm Playground with CodeMirror 6 & Tabulator</title>
<div class="main">
	<h1>DuckDB-Wasm Playground with CodeMirror 6 & Tabulator</h1>
	<CodeMirror
		bind:value
		lang={sql()}
		styles={{
			'&': {
				maxWidth: '100%',
				height: '20rem'
			}
		}}
	/>

	<button on:click={() => execute(value)}> Execute </button>

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

	button {
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
