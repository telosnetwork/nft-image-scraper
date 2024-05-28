import { sleep, pinCID, getPath } from "./util/utils.js";
import { handleForks, getPool, buildDatabase, LOCAL_QUERY_NFT, LOCAL_QUERY_LATEST, getRemoteQuery} from "./util/database.js";
import { createLogger } from "./util/logger.js";
import { exec, ExecException } from "child_process";

import PQueue from "p-queue";
import Scraper from "./scraper.js";
import { readFileSync } from 'fs'
import { ScraperConfig } from "./types/configs.js";
import pg, { QueryResult } from 'pg';
import { Logger } from "pino";

const configFile : URL = new URL('../config.json', import.meta.url);
const config : ScraperConfig = JSON.parse(readFileSync(configFile, 'utf-8'))

const logger : Logger = createLogger('NFTScraper main', config.logLevel);

const queueConcurrency : number = config.queueConcurrency || 16;
const queue : PQueue = new PQueue({concurrency: queueConcurrency});

const handleRemoteData = async (localPool: pg.Pool) : Promise<void> => {
    // Get latest local NFT
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
    // We should get only highest ? or all ? only highest should need to call for scraping
    // Rest should be loading it from local
    // But which remote is highest can change dynamically
    for(const database of config.databases){
        logger.debug(`Adding from remote ${database.name}...`);

        let data: pg.QueryResult<NFT> | undefined;
        let remotePool: pg.Pool;
        // Get rows from remote DB
        try {
            remotePool = await getPool(database);
            data = await remotePool.query<NFT>(getRemoteQuery(latestNFT));
            if(data.rowCount === 0){
                logger.debug(`No data to process found in ${database.host}:${database.name}`);
                continue;
            } 
            logger.debug(`${data.rowCount} NFT(s) found on ${database.host}:${database.name}...`);
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
                logger.error(`Error querying local database for row ${row.contract}:${row.token_id}: ${e}`);
                continue;
            }
            if(exists.rowCount > 0 && exists.rows[0].scraped){
                let row : NFT = exists.rows[0];
                if(row.scraped){
                    logger.debug(`Updating remote database ${database.host}:${database.name} with existing row ${row.contract}:${row.token_id}`);
                    const url : string = getPath(config, row.contract, row.token_id);
                    try {
                        await remotePool.query(`UPDATE nfts SET image_cache = $1 WHERE contract = $2 AND token_id = $3`, [url, row.contract, row.token_id]);
                    } catch(e: Error | any){
                        logger.error(`Error updating remote database for row ${row.contract}:${row.token_id}: ${e}`);
                    }
                    // TODO: add date comparaison so we retry scraping after a certain time, skip only if time hasn't elapsed.
                    continue; // Skip as already scraped
                }
            } else if(exists.rowCount > 0){
                if(exists.rows[0].scrub_count >= 100){
                    continue; // Skip if scrub count >= LIMIT
                }
            } else {
                logger.debug(`Inserting NFT ${row.contract}:${row.token_id} locally...`);
                // Get block hash from remote
                let blockHash: string;
                try {
                    const blockResult : pg.QueryResult = await remotePool.query(`SELECT * FROM blocks WHERE number = $1 LIMIT 1;`, [row.block_minted]);
                    if(blockResult.rowCount === 0){
                        logger.debug(`Block ${row.block_minted} not found in remote database ${database.host}:${database.name}`)
                        continue; // Skip if block not found
                    }
                    blockHash = blockResult.rows[0].hash;
                } catch(e: Error | any){
                    logger.error(`Error querying remote database block hash of ${row.block_minted} on ${database.host}:${database.name} : ${e}`);
                    continue; // Skip if block not found
                }

                // Insert into local database
                try {
                    await localPool.query(`
                        INSERT INTO nfts (block_minted, block_hash, contract, token_id, metadata, scrub_count, scrub_last, updated_at, scraped)
                        VALUES ($1, $2, $3, $4, $5, 0, NOW(), NULL, FALSE)
                        ON CONFLICT (contract, token_id) DO NOTHING
                    `, [row.block_minted, blockHash, row.contract, row.token_id, row.metadata]);
                    logger.info(`Inserted NFT ${row.contract}:${row.token_id} locally`);
                } catch(e: Error | any){
                    logger.error(`Error inserting in local database for row ${row.contract}:${row.token_id}: ${e}`);
                }
                // Don't skip. Continue with scraping...
            }

            // If we are running on an IPFS node try to pin
            if(config.localIpfs){
                try {
                    logger.debug(`Parsing CID from metadata for ${row.contract}:${row.token_id}...`);
                    // TODO: make sure this is needed rather than just pinCID call; Is it the parent CID / folder ?
                    let ipfsCID = row.metadata?.image?.match(/^(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[A-Za-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,}|F[0-9A-F]{50,})$/);
                    if(ipfsCID !== null){
                        let path : string = ipfsCID[0] + "/";
                        let cidParts : string[] = row.metadata?.image.split(path);
                        let ipfsCIDStr : string  = (cidParts.length > 1) ? path + cidParts[cidParts.length - 1] : ipfsCID[0];
                        logger.info(`Found CID ${ipfsCIDStr} for ${row.contract}:${row.token_id}. Pinning...`);
                        exec("export IPFS_PATH=/ipfs", (err : ExecException | null) => {
                            if(err){
                                logger.error("Could not set IPFS_PATH export path: " + err);
                            } else {
                                exec("ipfs pin add " + ipfsCIDStr, (e : ExecException | null) => {
                                    if(e){
                                        logger.error("Could not pin content to IPFS with CID "  + ipfsCIDStr + ": " +  e);
                                    }
                                });
                            }
                        });
                    }
                    pinCID(row);
                } catch (e: Error | any) {
                    logger.error(`Exception while pinning NFT: ${e} \n\n ${JSON.stringify(row, null, 4)}`)
                }
            }
        }
    }
}


const fillQueue = async (localPool: pg.Pool) : Promise<void> => {
    let results : pg.QueryResult<NFT> = { command: '', rowCount: 0, oid: 0, fields: [], rows: [] };
    try {
        results = await localPool.query(`
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
            ORDER BY scrub_count ASC LIMIT 500;
        `);
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