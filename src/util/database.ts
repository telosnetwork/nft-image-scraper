import pg from 'pg';
import {ScraperConfig, DatabaseConfig} from "../types/configs.js";

const configFile = new URL('../../config.json', import.meta.url);
const config: ScraperConfig = JSON.parse(readFileSync(configFile, 'utf-8'));
let pools : any = {};

import {readFileSync} from 'fs'
import { Logger } from 'pino';

export const LOCAL_QUERY_NFT = `SELECT * FROM nfts WHERE contract = $1 AND token_id = $2`;
export const LOCAL_QUERY_LATEST = `SELECT * FROM nfts ORDER BY block_minted DESC LIMIT 1;`;

export function getRemoteQuery (block: number = 0) : string {
    return `SELECT *
        FROM nfts
        WHERE (image_cache = '' OR image_cache IS NULL)
        AND metadata IS NOT NULL
        AND metadata::text != '"___INVALID_METADATA___"'::text
        ORDER BY block_minted DESC
        LIMIT ${config.querySize || 50}
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

export async function buildDatabase(config: ScraperConfig) : Promise<pg.Pool> {
    try {
        let pool: pg.Pool = await getPool(config.database);
        const query = `
            CREATE TABLE IF NOT EXISTS nfts (
                block_minted BIGINT NOT NULL,
                block_hash VARCHAR(66) NOT NULL,
                contract VARCHAR(42),
                token_id VARCHAR(125),
                scrub_count INT DEFAULT 0,
                scrub_last TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                scraped BOOLEAN DEFAULT FALSE,
                metadata TEXT NOT NULL,
                PRIMARY KEY (contract, token_id)
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
            // Get latest nft mint
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
                    await remotePool.query(`UPDATE nfts SET image_cache = NULL WHERE contract = $1 AND token_id = $2`, [row.contract, row.token_id]);
                }
            }
        }  catch(e: Error | any){
            logger.error(`Error deleting image cache on remotes: ${e}`);
        }
    }
};