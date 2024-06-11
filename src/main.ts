import { sleep, pinIPFS, getPath } from "./util/utils.js";
import { handleForks, getPool, buildDatabase, LOCAL_QUERY_NFT, LOCAL_QUERY_LATEST, getRemoteQuery, getLatestNFTBlock, insertNFT, getBlockHash, updateRemote, LOCAL_QUERY_SCRAP} from "./util/database.js";
import { createLogger } from "./util/logger.js";

import PQueue from "p-queue";
import Scraper from "./scraper.js";
import { readFileSync } from 'fs'
import { ScraperConfig } from "./types/configs.js";
import pg, { Pool } from 'pg';
import { Logger } from "pino";

const configFile : URL = new URL('../config.json', import.meta.url);
const config : ScraperConfig = JSON.parse(readFileSync(configFile, 'utf-8'))

const logger : Logger = createLogger('NFT Scraper :: main.js', config.logLevel);

const queueConcurrency : number = config.queueConcurrency || 16;
const queue : PQueue = new PQueue({concurrency: queueConcurrency});

const handleRemoteData = async (localPool: pg.Pool) : Promise<void> => {
    // Get latest local NFT
    const latestNFT : number = await getLatestNFTBlock(localPool, logger);
    handleRemoteERC(localPool, latestNFT, 'nfts');
    handleRemoteERC(localPool, latestNFT, 'erc1155');
}

const handleRemoteERC = async (localPool: Pool, latestNFT: number, table: string = 'nfts') : Promise<void> => {
    const type = (table === 'nfts') ? 721 : 1155;
    for(const database of config.databases){
        logger.debug(`Adding ERC${type} from remote ${database.name}...`);

        let data: pg.QueryResult<NFT> | undefined;
        let remotePool: pg.Pool;
        // Get rows from remote DB
        try {
            remotePool = await getPool(database);
            data = await remotePool.query<NFT>(getRemoteQuery(latestNFT, table));
            if(data.rowCount === 0){
                logger.debug(`No data to process found in ${database.host}:${database.name}`);
                continue;
            } 
            logger.debug(`${data.rowCount} ERC${type}(s) found on ${database.host}:${database.name}...`);
        } catch(e) {
            logger.error(`Error querying database ${database.host}:${database.name} : ${e}`);
            continue;
        }

        for (const row of data?.rows) {
            // Check local database for nft
            let exists : pg.QueryResult<NFT>
            try {
                exists = await localPool.query(LOCAL_QUERY_NFT , [row.contract, row.token_id]);
            } catch(e: Error | any){
                logger.error(`Error querying local database for ERC${type} ${row.contract}:${row.token_id}: ${e}`);
                continue;
            }
            if(exists.rowCount > 0 && exists.rows[0].scraped){
                let row : NFT = exists.rows[0];
                if(row.scraped){
                    await updateRemote(remotePool, database,  row, type, table, logger);
                    // TODO: add date comparaison so we retry scraping after a certain time, skip only if time hasn't elapsed.
                    continue; // Skip as already scraped
                }
            } else if(exists.rowCount > 0){
                if(exists.rows[0].scrub_count >= 100){
                    continue; // Skip if scrub count >= LIMIT
                }
            } else {
                logger.debug(`Inserting ERC${type} ${row.contract}:${row.token_id} locally...`);
                // Get block hash from remote
                logger.debug(`Getting block hash for ${row.block_created || row.block_minted }`);
                let blockHash : string = await getBlockHash(remotePool, database, row, logger);
                if(blockHash === ''){
                    continue;
                }
                await insertNFT(localPool, row, blockHash, type, logger);
            }
            pinIPFS(row, logger);
        }
    }
}


const fillQueue = async (localPool: pg.Pool) : Promise<void> => {
    let results : pg.QueryResult<NFT> = { command: '', rowCount: 0, oid: 0, fields: [], rows: [] };
    try {
        results = await localPool.query(LOCAL_QUERY_SCRAP);
    } catch(e: Error | any){
        logger.error(`Error getting NFTs from local database: ${e}`);
    }
    for(const row of results.rows){
        // Add scrap to queue
        try {
            logger.info(`Adding scraping task for ${row.contract}:${row.token_id}`);
            queue.add(async () => {
                logger.debug(`Running scraping task for ${row.contract}:${row.token_id}`);
                // Scrap and resize
                try {
                    const scraper : Scraper = new Scraper(localPool, row, config, logger);
                    await scraper.scrapeAndResize();
                    logger.info(`Scraping ${row.contract}:${row.token_id} complete`)
                } catch (e: Error | any) {
                    logger.error(`Error running scraper: ${e.message}`)
                }
            })
        } catch (e: Error | any) {
            logger.error(`Exception while scraping NFT: ${e.message} \n\n ${JSON.stringify(row, null, 4)}`)
        }
    }
}
;(async () : Promise<void> => {
    logger.debug(`Building database...`);
    const localPool : pg.Pool = await buildDatabase(config);
    while (true) {
        logger.debug(`Checking forks...`);
        await handleForks(localPool, logger);
        logger.debug(`Parsing NFTs from remote databases...`);
        await handleRemoteData(localPool);
        logger.debug(`Filling queues...`);
        await fillQueue(localPool);
        logger.debug(`Sleeping...`);
        await sleep(5000);
        logger.debug(`Done sleeping, size of queue is ${queue.size}`);
        await queue.onSizeLessThan(queueConcurrency);
        logger.debug(`Queue size less than ${queueConcurrency}`);
    }
})().catch((e : Error |  any) => {
    logger.error(`Error while running scraper: ${e}`);
})