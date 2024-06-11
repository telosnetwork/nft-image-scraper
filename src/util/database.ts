import pg from 'pg';
import {ScraperConfig, DatabaseConfig} from "../types/configs.js";
import {readFileSync} from 'fs'
import { Logger } from 'pino';
import { getPath } from './utils.js';

const configFile = new URL('../../config.json', import.meta.url);
const config: ScraperConfig = JSON.parse(readFileSync(configFile, 'utf-8'));
let pools : any = {};

export const LOCAL_QUERY_NFT = `SELECT * FROM nfts WHERE contract = $1 AND token_id = $2`;
export const LOCAL_QUERY_LATEST = `SELECT * FROM nfts ORDER BY block_minted DESC LIMIT 1;`;
export const LOCAL_QUERY_SCRAP = `
SELECT * FROM nfts 
WHERE scraped = FALSE 
AND ( 
    scrub_count < 3
    OR (scrub_last < NOW() - INTERVAL '1 hour' AND scrub_count < 12)
    OR (scrub_last < NOW() - INTERVAL '2 hours' AND scrub_count < 25)
    OR (scrub_last < NOW() - INTERVAL '6 hours' AND scrub_count < 40)
    OR (scrub_last < NOW() - INTERVAL '12 hours' AND scrub_count < 50)
    OR (scrub_last < NOW() - INTERVAL '1 day' AND scrub_count < 60)
    OR (scrub_last < NOW() - INTERVAL '3 days' AND scrub_count < 70)
    OR (scrub_last < NOW() - INTERVAL '7 days' AND scrub_count < 90)
    OR (scrub_last < NOW() - INTERVAL '14 days' AND scrub_count < 100)
)
ORDER BY scrub_count ASC LIMIT ${config.querySize || 500};
`

export function getRemoteQuery (block: number = 0, table: string = 'nfts') : string {
    let orderField = (table === 'nfts') ? 'block_minted' : 'block_created';
    return `SELECT *
        FROM ${table}
        WHERE (image_cache = '' OR image_cache IS NULL)
        AND ${orderField} IS NOT NULL
        AND metadata IS NOT NULL
        AND metadata::text != '"___INVALID_METADATA___"'::text
        ORDER BY ${orderField} DESC
        LIMIT ${config.querySize || 500}
    `;
}

export async function getPool(database: DatabaseConfig): Promise<pg.Pool> {
    if(!pools[`${database.host}:${database.name}`]){
        pools[`${database.host}:${database.name}`] = new pg.Pool({
            database: database.name,
            user: database.user,
            password: database.password,
            host: database.host,
            port: database.port,
        });
    }
    return pools[`${database.host}:${database.name}`];
}

export async function updateRemote(remotePool: pg.Pool, database: DatabaseConfig, row: NFT, type: number, table: string, logger: Logger){
    
    logger.debug(`Updating remote database ${database.host}:${database.name} with existing ERC${type} ${row.contract}:${row.token_id}`);
    const url : string = getPath(config, row.contract, row.token_id);
    try {
        await remotePool.query(`UPDATE ${table} SET image_cache = $1 WHERE contract = $2 AND token_id = $3`, [url, row.contract, row.token_id]);
    } catch(e: Error | any){
        logger.error(`Error updating remote database for ERC${type} ${row.contract}:${row.token_id}: ${e}`);
    }
}
export async function getBlockHash(remotePool: pg.Pool, database: DatabaseConfig, row: NFT, logger: Logger) : Promise<string> {
    let blockHash: string = '';
    let block: number = (row.block_minted) ? row.block_minted : row.block_created;
    if(!block){
        logger.error(`No block found for ${row.contract}:${row.token_id}`);
    }
    try {
        const blockResult : pg.QueryResult = await remotePool.query(`SELECT * FROM blocks WHERE number = $1 LIMIT 1;`, [block]);
        if(blockResult.rowCount === 0){
            logger.debug(`Block ${row.block_minted} not found in remote database ${database.host}:${database.name}`)
        } else {
            blockHash = blockResult.rows[0].hash;
        }
    } catch(e: Error | any){
        logger.error(`Error querying remote database block hash of ${row.block_minted} on ${database.host}:${database.name} : ${e}`);
    }
    return blockHash;
}
export async function insertNFT(localPool: pg.Pool, row: NFT, blockHash: string, type: number, logger: Logger){
    // Insert into local database or reset scrap status
    try {
        await localPool.query(`
            INSERT INTO nfts (block_minted, block_hash, contract, token_id, metadata, erc, scrub_count, scrub_last, updated_at, scraped)
            VALUES ($1, $2, $3, $4, $5, $6, 0, NOW(), NULL, FALSE)
            ON CONFLICT (contract, token_id) 
            DO UPDATE SET scraped = FALSE, scrub_count = 0;
        `, [row.block_minted, blockHash, row.contract, row.token_id, row.metadata, type]);
        logger.info(`Inserted ERC721 ${row.contract}:${row.token_id} locally`);
    } catch(e: Error | any){
        logger.error(`Error inserting in local database for row ${row.contract}:${row.token_id}: ${e}`);
    }
}

export async function buildDatabase(config: ScraperConfig) : Promise<pg.Pool> {
    try {
        let pool: pg.Pool = await getPool(config.database);
        const query = `
            CREATE TABLE IF NOT EXISTS nfts (
                contract VARCHAR(42),
                token_id VARCHAR(125),
                block_minted BIGINT NOT NULL,
                block_hash VARCHAR(66) NOT NULL,
                scrub_count INT DEFAULT 0,
                scrub_last TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                scraped BOOLEAN DEFAULT FALSE,
                metadata TEXT NOT NULL,
                erc INT DEFAULT 721,
                UNIQUE (contract, token_id)
            );
        `;
        await pool.query(query);
        return pool;
    } catch(e: Error | any){
        throw `Error building database: ${e.message}`;
    }
}

export async function getLastCorrectBlock (localPool: pg.Pool, logger : Logger) : Promise<number> {
    let i : number = 1;
    logger.debug(`Getting last correct block...`);
    while(true){
        try {
            // TODO: Get both latest erc20 & erc1155 mints
            const results : pg.QueryResult<NFT> = await localPool.query(`SELECT * FROM nfts ORDER BY block_minted DESC LIMIT ${i * 100};`);
            if(results.rowCount === 0){
                return 0;
            }

            // Get the highest block number from the remote databases
            let highestBlock : number = 0;
            let mainDatabase : DatabaseConfig | undefined = undefined;
            for(const database of config.databases){
                const remotePool : pg.Pool = await getPool(database);
                const blockResult : pg.QueryResult = await remotePool.query(`SELECT * FROM blocks ORDER BY number DESC LIMIT 1;`);
                if(blockResult.rowCount === 0){
                    continue;
                }
                if(blockResult.rows[0].number > highestBlock){
                    highestBlock = blockResult.rows[0].number;
                    mainDatabase = database;
                }
            }

            if(mainDatabase){
                logger.debug(`Database with highest block: ${mainDatabase.host}:${mainDatabase.name} with block ${highestBlock}`);
                const remotePool : pg.Pool = await getPool(mainDatabase);

                // Check if the block hashes match
                for(const nft of results.rows){
                    try {
                        const resultsRemote : pg.QueryResult = await remotePool.query(`SELECT * FROM blocks WHERE number = $1 LIMIT 1;`, [nft.block_minted]);
                        if(resultsRemote.rowCount === 0){
                            continue;
                        }
                        const remoteBlock = resultsRemote.rows[0];    
                        if(remoteBlock.hash === nft.block_hash){
                            // Correct block found  
                            return remoteBlock.number;
                        } else {
                            logger.debug(`Block ${nft.block_minted} hash does not match in ${mainDatabase.name}. Fork detected, continuing until we have a correct block...`);
                        }
                    } catch(e: Error | any){
                        logger.error(`Error getting block hash for block ${nft.block_minted} in ${mainDatabase.name}: ${e.message}`);
                    }
                }
            }
            i++;
        } catch(e){
            logger.error(`Error getting latest nft: ${e}`);
        }
    }
}
export async function getLatestNFTBlock(localPool: pg.Pool, logger: Logger) : Promise<number> {
    let latestNFT : number = 0;
    try {
        const results = await localPool.query(LOCAL_QUERY_LATEST);
        if(results.rowCount > 0){
            latestNFT = results.rows[0].block_minted;
            logger.debug(`Latest NFT found locally at block: ${latestNFT}`);
        }
    } catch(e){
        logger.error(`Error getting latest nft: ${e}`);
    }
    return latestNFT;
}
export async function handleForks (localPool: pg.Pool, logger : Logger) : Promise<void> {
    const last = await getLastCorrectBlock(localPool, logger);
    logger.debug(`Last correct block with NFT: ${last}`);
    if(last && last > 0){
        let results : pg.QueryResult<NFT>;
        // Select the nfts from local 
        try {
           results = await localPool.query(`SELECT * FROM nfts WHERE block_minted > $1`, [last]);
            
            if(results.rowCount === 0){
                return;
            }
            logger.debug(`Found ${results.rowCount} nfts older than ${last} locally...`);
            
        } catch(e: Error | any){
            logger.error(`Error selecting nfts after block ${last}: ${e}`);
            return;
        }

        // Delete the nfts from local 
        try {
            await localPool.query(`DELETE FROM nfts WHERE block_minted > $1`, [last]);
            logger.info(`Deleted nfts older than ${last} on local ...`);
        } catch(e: Error | any){
            logger.error(`Error deleting nfts after block ${last}: ${e}`);
        }

        // Set image_cache to null for selected nfts on remotes
        try {
            logger.info(`Deleting image caches on ${config.databases.length} remote database(s)...`);
            for(const database of config.databases){
                let remotePool : pg.Pool = await getPool(database);
                for(const row of results.rows){
                    logger.debug(`Deleting image cache for ${row.contract} ${row.token_id} from remote database ${database.name}...`);
                    const table = (row.erc === 721) ? 'nfts' : 'erc_1155';
                    await remotePool.query(`UPDATE ${table} SET image_cache = NULL WHERE contract = $1 AND token_id = $2`, [row.contract, row.token_id]);
                }
            }
        }  catch(e: Error | any){
            logger.error(`Error deleting image cache on remotes: ${e}`);
        }
    }
};