var snowflake = require('snowflake-sdk');

const SNOWFLAKE_ACCOUNT = process.env.SNOWFLAKE_ACCOUNT;
const SNOWFLAKE_DATABASE = process.env.SNOWFLAKE_DATABASE;
const SNOWFLAKE_TOKEN = process.env.SNOWFLAKE_TOKEN;
const SNOWFLAKE_USERNAME = process.env.SNOWFLAKE_USERNAME;

function execute(conn, sqlText, binds = []) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => {
        console.log(sqlText, binds);
        if (err) {
          reject(err)
        }
        resolve(rows)
      },
    })
  })
}

function connect(connection) {
  return new Promise((resolve, reject) => {
    connection.connect(function (err, _conn) {
      if (err) {
        reject(err)
      } else {
        resolve(connection)
      }
    })
  })
}

const SELECT_STORES = `SELECT * FROM stores;`;
const SYSTEM_WAIT = `CALL SYSTEM$WAIT(3);`;

// Connection limit
async function findConnectionLimit() {
  const conns = [];

  while (true) {
    console.log('Creating connection...');
    const connection = snowflake.createConnection({
      account: SNOWFLAKE_ACCOUNT,
      username: SNOWFLAKE_USERNAME,
      authenticator: 'OAUTH',
      token: SNOWFLAKE_TOKEN,
      database: SNOWFLAKE_DATABASE,
    });

    console.log('Connecting...');
    await connect(connection);
 
    conns.push(connection);

    console.log(`Verifying ${conns.length} connections...`);
    await Promise.all(conns.map(c => c.heartbeatAsync()));

    console.log(`Querying ${conns.length} connections...`);
    await Promise.all(conns.map(c => execute(c, SYSTEM_WAIT)));
  }
}

// Concurrent queries on same connection
async function runConncurrentQueries() {
  console.log('Creating connection...');
  const connection = snowflake.createConnection({
    account: SNOWFLAKE_ACCOUNT,
    username: SNOWFLAKE_USERNAME,
    authenticator: 'OAUTH',
    token: SNOWFLAKE_TOKEN,
    database: SNOWFLAKE_DATABASE,
  });

  console.log('Connecting...');
  await connect(connection);
 
  queries = [];
  while (true) {
    if (queries.length % 2 === 0) {
      queries.push(SYSTEM_WAIT);
    } else {
      queries.push(SELECT_STORES);
    }

    console.log(`Verifying connection...`);
    await connection.heartbeatAsync();

    console.log(`Executing ${queries.length} queries on one connection...`);
    await Promise.all(queries.map((q, index) => execute(connection, q, [index])));
  }
}


(async () => {
    try {
        await findConnectionLimit();
        process.exit(0);
    } catch (e) {
        console.error(e)
        process.exit(1);
    }
})();
